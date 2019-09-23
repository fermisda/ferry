package main
import (
	"crypto/x509"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"bytes"
	"bufio"
	"strings"
	"crypto/tls"
	"github.com/spf13/viper"
	"crypto/x509/pkix"
)

// we probably need   list of authorized DNs too
func createDNlist (filename string)  ( []string, error) {
	var DNlist []string
	f, err := os.Open(filename)

	if err != nil {
		return DNlist,err
	}
	scanner := bufio.NewScanner(f)
	defer f.Close()
	for scanner.Scan() {
		DNlist = append(DNlist,scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading authorized DN file " + filename + ".")
	}

	return DNlist,err
}
func loadCerts(certs []string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	for _, ca := range certs {
		f, err := os.Open(ca)
		if err != nil {
			fmt.Println("Something went wrong opening " + ca) 
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
	ip := strings.Split(c.R.RemoteAddr, ":")[0]
	authIPs := viper.GetStringSlice("ip_whitelist")
	authDNs := viper.GetStringSlice("dn_whitelist")
	dnRoles	:= viper.GetStringMapStringSlice("dn_roles")
	ipRoles := viper.GetStringMapStringSlice("ip_roles")

	for _, presCert := range c.R.TLS.PeerCertificates {
		certDN := ParseDN(presCert.Subject.Names, "/")

		// authorize public role
		if r == "public" {
			return LevelPublic, fmt.Sprintf("public role authorized DN %s", certDN)
		}

		// authorize DN roles
		if roles, ok := dnRoles[strings.ToLower(certDN)]; ok {
			for _, role := range roles {
				if r.String() == role {
					return LevelDNRole, fmt.Sprintf("cert matches authorized role %s DN %s", role, certDN)
				}
			}
		}

		// authorize whitelisted DNs
		for _, authDN := range authDNs {
			if authDN == certDN {
				return LevelDNWhitelist, fmt.Sprintf("cert matches whitelisted DN %s", certDN)
			}
		}
	}

	// authorize IP roles
	if roles, ok := ipRoles[ip]; ok {
		for _, role := range roles {
			if r.String() == role {
				return LevelIPRole, fmt.Sprintf("ignoring DN of authorized IP %s with role %s", ip, role)
			}
		}
	}

	// authorize whitelisted IPs
	for _, authIP := range authIPs {
		if authIP == ip {
			return LevelIPWhitelist, fmt.Sprintf("ignoring DN of whitelisted IP %s", ip)
		}
	}

	return LevelDenied, "unable to authorize access"
}

func checkClientIP(client *tls.ClientHelloInfo) (*tls.Config, error) {
	ip := client.Conn.RemoteAddr().String()

	authIPs := viper.GetStringSlice("ip_whitelist")
	ipRoles := viper.GetStringMapStringSlice("ip_roles")
	for ip := range ipRoles {
		authIPs = append(authIPs, ip)
	}

	for _, authIP := range authIPs {
		if authIP == strings.Split(ip, ":")[0] {
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
	}

	return nil, nil
}
