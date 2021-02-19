package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/patrickmn/go-cache"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var accCache *cache.Cache

type accessor struct {
	accid   int
	name    string
	active  bool
	write   bool
	accType string
}

func queryAccessors(key string) (accessor, bool) {
	var found = true
	var acc accessor

	// Not using the transaction.go logic as it is written to be called with Run(....)

	ctx := context.Background()

	tx, err := DBptr.BeginTx(ctx, nil)
	if err != nil {
		log.Error("quer2yAccessors - begin transaction")
		log.Error(err)
		return acc, false
	}

	err = tx.QueryRow(`select accid, name, active, write, type from accessors where name = $1 and active = true`,
		key).Scan(&acc.accid, &acc.name, &acc.active, &acc.write, &acc.accType)
	if err == sql.ErrNoRows {
		found = false
	} else if err != nil {
		log.Error("queryAccessors - select")
		log.Error(err)
		found = false
	}
	// So we can know who is using FERRY
	if found {
		_, err = tx.ExecContext(ctx, `update accessors set last_used=NOW() where accid = $1`, acc.accid)
		if err != nil {
			log.Error("queryAccessors - update")
			log.Error(err)
			found = false
		}
	}

	if found == true {
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

func getAccessor(key string) (accessor, bool) {
	var found bool
	var acc accessor

	data, found := AccCache.Get(key)
	if found {
		acc = data.(accessor)
		//log.Info(fmt.Sprintf("Accessors: Key found in cache.  Key: %s:", key))
	} else {
		acc, found = queryAccessors(key)
		//log.Info(fmt.Sprintf("Accessors: Queried database for : %s  Key found: %t", key, found))
		if found == true {
			AccCache.Set(key, acc, cache.DefaultExpiration)
		}
	}

	return acc, found
}

// we probably need   list of authorized DNs too
func createDNlist(filename string) ([]string, error) {
	var DNlist []string
	f, err := os.Open(filename)

	if err != nil {
		return DNlist, err
	}
	scanner := bufio.NewScanner(f)
	defer f.Close()
	for scanner.Scan() {
		DNlist = append(DNlist, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading authorized DN file " + filename + ".")
	}

	return DNlist, err
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

//ParseDN parses a []pkix.AttributeTypeAndValue into a string.
func ParseDN(names []pkix.AttributeTypeAndValue, sep string) string {
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

func authorize(c APIContext, r AccessRole) (AccessLevel, string) {

	// Try authorizing the Certs
	for _, presCert := range c.R.TLS.PeerCertificates {
		certDN := ParseDN(presCert.Subject.Names, "/")

		acc, found := getAccessor(certDN)
		if found == false {
			continue
		} else if acc.accType == "dn_role" {
			// authorize DN roles
			if r == RoleRead || (r == RoleWrite && acc.write) {
				return LevelDNRole, fmt.Sprintf("cert matches authorized role %s DN %s", r, certDN)
			}
		} else if acc.accType == "dn_whitelist" {
			// authorize Whitelisted DN s
			return LevelDNWhitelist, fmt.Sprintf("cert matches whitelisted DN %s", certDN)
		}
	}

	// Try authorizing the  IP address
	ip := strings.Split(c.R.RemoteAddr, ":")[0]
	acc, found := getAccessor(ip)
	if found {
		// authorize IP roles
		if acc.accType == "ip_role" {
			if r == RoleRead || (r == RoleWrite && acc.write) {
				return LevelIPRole, fmt.Sprintf("ignoring DN of authorized IP %s with role %s", ip, r)
			}
			// authorize whitelisted IPs
		} else if acc.accType == "ip_whitelist" {
			return LevelIPWhitelist, fmt.Sprintf("ignoring DN of whitelisted IP %s", ip)
		}
	}

	// See, if the API allows public access
	if r == RolePublic {
		return LevelPublic, fmt.Sprintf("public role authorized")
	}

	// Go away, we don't like you
	return LevelDenied, "unable to authorize access"
}

func checkClientIP(client *tls.ClientHelloInfo) (*tls.Config, error) {
	ip := client.Conn.RemoteAddr().String()
	ip = strings.Split(ip, ":")[0]
	_, found := getAccessor(ip)

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
