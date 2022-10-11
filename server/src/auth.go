package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/lestrrat-go/jwx/jwt"

	"github.com/google/uuid"
	"github.com/patrickmn/go-cache"
	scitokens "github.com/scitokens/scitokens-go"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var accCache *cache.Cache
var enforcer scitokens.Enforcer

type accessor struct {
	accid   int
	name    string
	active  bool
	write   bool
	accType string
	uname   string
}

func AuthInitialize() error {
	if enforcer == nil {
		ctx := context.Background()
		if len(viper.GetStringSlice("issuers")) == 0 {
			return errors.New("at least one 'issuers' must exist in the config file")
		}
		var err error
		enforcer, err = scitokens.NewEnforcerDaemon(ctx, viper.GetStringSlice("issuers")...)
		if err != nil {
			return fmt.Errorf("auth.AuthInitilize NewEnforcerDeamon error: %s", err.Error())
		}
	}
	return nil
}

func queryAccessors(key string) (accessor, bool) {
	var found = true
	var acc accessor

	// Not using the transaction.go logic as it is written to be called with Run(....)

	ctx := context.Background()

	tx, err := DBptr.BeginTx(ctx, nil)
	if err != nil {
		log.Error(fmt.Sprintf("queryAccessors - transaction failed: %s", err.Error()))
		log.Error(err)
		return acc, false
	}

	err = tx.QueryRow(`select accid, name, active, write, type from accessors where name = $1 and active = true`,
		key).Scan(&acc.accid, &acc.name, &acc.active, &acc.write, &acc.accType)
	if err == sql.ErrNoRows {
		found = false
	} else if err != nil {
		log.Error(fmt.Sprintf("queryAccessors - query failed: %s", err.Error()))
		log.Error(err)
		found = false
	}

	// Find out who the UUID belongs to.
	if acc.accType == "jwt_role" {
		// Test if this is a UUID.  If not, it is still valid as it is in the accessors table.  But, they are coming in as a
		// *pro account (novapro for example) where the subject is XXXpro@fnal.gov not a UUID.  Those are not in the users table,
		// but are in LDAP through the capability set of that name.
		_, err = uuid.Parse(acc.name)
		if err == nil {
			err = tx.QueryRow(`select uname from users where voPersonId = $1`, acc.name).Scan(&acc.uname)
			if err == sql.ErrNoRows {
				log.Error(fmt.Sprintf("queryAccessors - uuid does not reference a valid user.  uuid: %s", acc.name))
				found = false
			} else if err != nil {
				log.Error(fmt.Sprintf("queryAccessors - query failed (2): %s", err.Error()))
				found = false
			}
		}
	}

	// Update, so we can know who is using FERRY
	if found {
		_, err = tx.ExecContext(ctx, `update accessors set last_used=NOW() where accid = $1`, acc.accid)
		if err != nil {
			log.Error(fmt.Sprintf("queryAccessors - update: %s", err.Error()))
			found = false
		}
	}

	if found {
		err = tx.Commit()
	} else {
		err = tx.Rollback()
	}
	if err != nil {
		log.Error("queryAccessors - Commit")
		log.Error(err)
		found = false
	}

	return acc, found
}

func getAccessor(key string, ip string) (accessor, bool) {
	var acc accessor
	var whereFound string = "NOT FOUND"
	var found bool = false

	data, found := AccCache.Get(key)
	if found {
		acc = data.(accessor)
		whereFound = "cache"
	} else {
		acc, found = queryAccessors(key)
		if found {
			// Store in cache by BOTH DN, if provided, and IP. Be aware that checkClientIP checks the IP,
			// regardless if DN is used.   Not caching both will cause a DB hit on every API called via a DN.
			//
			// Set() REPLACES any existing matching item -- there may be one of the two still in cache.
			AccCache.Set(key, acc, cache.DefaultExpiration)
			if key != ip {
				AccCache.Set(ip, acc, cache.DefaultExpiration)
			}
			whereFound = "accessors table"
		}
	}
	log.Debug(fmt.Sprintf("getAccessor key %s -- found in: %s", key, whereFound))
	return acc, found
}

func loadCerts(certs []string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	for _, ca := range certs {
		f, err := os.Open(ca)
		if err != nil {
			fmt.Println("loadCerts: Something went wrong opening " + ca)
			return nil, err
		}
		defer f.Close()
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(f); err != nil {
			return nil, err
		}
		if ok := pool.AppendCertsFromPEM(buf.Bytes()); !ok {
			return nil, err
		}
	}
	return pool, nil
}

//parseDN parses a []pkix.AttributeTypeAndValue into a string.
func parseDN(names []pkix.AttributeTypeAndValue, sep string) string {
	var oid = map[string]string{
		"2.5.4.3":                    "CN",
		"2.5.4.4":                    "SN",
		"2.5.4.5":                    "serialNumber",
		"2.5.4.6":                    "C",
		"2.5.4.7":                    "L",
		"2.5.4.8":                    "ST",
		"2.5.4.9":                    "STREET",
		"2.5.4.10":                   "O",
		"2.5.4.11":                   "OU",
		"2.5.4.12":                   "title",
		"2.5.4.17":                   "postalCode",
		"2.5.4.42":                   "GN",
		"2.5.4.43":                   "initials",
		"2.5.4.44":                   "generationQualifier",
		"2.5.4.46":                   "dnQualifier",
		"2.5.4.65":                   "pseudonym",
		"0.9.2342.19200300.100.1.25": "DC",
		"1.2.840.113549.1.9.1":       "emailAddress",
		"0.9.2342.19200300.100.1.1":  "UID",
	}

	var subject []string
	for _, i := range names {
		subject = append(subject, fmt.Sprintf("%s=%s", oid[i.Type.String()], i.Value))
	}
	return sep + strings.Join(subject, sep)
}

// Authorize the client by JWT, DN or IP.  Returns the level of access, a message and the validated string.
func authorize(c APIContext, r AccessRole) (AccessLevel, string, string) {

	// See, if the API allows public access
	if r == RolePublic {
		return LevelPublic, "public role authorized", ""
	}

	ip := strings.Split(c.R.RemoteAddr, ":")[0]

	// Try authorizing using a json web token
	_, err := jwt.ParseRequest(c.R)
	if err == nil {
		var uuid string
		if token, err := enforcer.ValidateTokenRequest(c.R); err == nil {
			uuid = token.Subject()
		} else {
			e := &scitokens.TokenValidationError{}
			if !errors.As(err, &e) {
				// some internal error while parsing/validating the token
				log.Error(err)
				return LevelDenied, fmt.Sprintf("Internal scitokens error: %s", err.Error()), ""
			} else {
				// token is not valid, err (and e.Err) will say why.
				log.Info(err)
				return LevelDenied, e.Error(), ""
			}
		}
		acc, found := getAccessor(uuid, ip)
		if found {
			// authorize JWT roles
			if acc.accType == "jwt_role" {
				if r == RoleRead || (r == RoleWrite && acc.write) {
					return LevelDNRole, fmt.Sprintf("JWT matches authorized role %s UUID %s for %s", r, uuid, acc.uname), fmt.Sprintf("%s %s", acc.uname, uuid)
				}
			}
		}
	}

	// Try authorizing DN by the Certs
	for _, presCert := range c.R.TLS.PeerCertificates {
		certDN := parseDN(presCert.Subject.Names, "/")
		acc, found := getAccessor(certDN, ip)
		if found {
			// authorize DN roles
			if acc.accType == "dn_role" {
				if r == RoleRead || (r == RoleWrite && acc.write) {
					return LevelDNRole, fmt.Sprintf("cert matches authorized role %s DN %s", r, certDN), certDN
				}
			}
		}
	}

	// Try authorizing by the  IP address
	acc, found := getAccessor(ip, ip)
	if found {
		// authorize IP roles
		if acc.accType == "ip_role" {
			if r == RoleRead || (r == RoleWrite && acc.write) {
				return LevelIPRole, fmt.Sprintf("ignoring DN of authorized IP %s with role %s", ip, r), ip
			}
		}
	}

	// Go away, we don't like you
	return LevelDenied, "unable to authorize access", ""
}

func checkClientIP(client *tls.ClientHelloInfo) (*tls.Config, error) {
	ip := client.Conn.RemoteAddr().String()
	ip = strings.Split(ip, ":")[0]
	_, found := getAccessor(ip, ip)

	if found {
		log.WithFields(log.Fields{"client": ip}).Info("Host matches authorized IP.")

		var err error
		srvConfig := viper.GetStringMapString("server")
		newConfig := Mainsrv.TLSConfig.Clone()
		newConfig.ClientAuth = tls.VerifyClientCertIfGiven
		newConfig.Certificates = make([]tls.Certificate, 1)
		newConfig.Certificates[0], err = tls.LoadX509KeyPair(srvConfig["cert"], srvConfig["key"])
		if err != nil {
			log.Print(err)
			return nil, err
		}

		return newConfig, nil
	}

	return nil, nil
}
