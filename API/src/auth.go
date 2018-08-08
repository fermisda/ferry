package main
import (
	"crypto/x509"
	"fmt"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"bytes"
	"bufio"
	"strings"
	"crypto/tls"
	"github.com/spf13/viper"
	"crypto/x509/pkix"
	"time"
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
		"2.5.4.9":                    "streetAddress",
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
		"0.9.2342.19200300.100.1.1":  "userid",
	}

	var subject []string
	for _, i := range names {
		subject = append(subject, fmt.Sprintf("%s=%s", oid[i.Type.String()], i.Value))
	}
	return sep + strings.Join(subject, sep)
}

func authorize( req *http.Request ) (bool, string) {
	thetime := time.Now()
	ip := req.RemoteAddr
	authIPs := viper.GetStringSlice("ip_whitelist")
	authDNs := viper.GetStringSlice("dn_whitelist")

	// authorization should fail by default
	authorized := false

	// ignore DN if host matches authorized an ip
	for _, authIP := range authIPs {
		if authIP == strings.Split(ip, ":")[0] {
			log.WithFields(QueryFields(req, thetime)).Print("Ignoring DN of authorized IP.")
			return true, "Ignoring DN of authorized IP."
		}
	}

	// string to build the full DN
	certDN := ""
	for _, presCert := range req.TLS.PeerCertificates {
		certDN = ParseDN(presCert.Subject.Names, "/")
		for _, authDN := range authDNs {
			if authDN == certDN {
				authorized = true
				log.WithFields(QueryFields(req, thetime)).Print("Cert matches authorized DN " + certDN)
			}
		}
	}
	if certDN != "" {
		return authorized, certDN
	} else {
		return authorized, "No CN found or no cert presented."	
	}
	
}

func checkClientIP(client *tls.ClientHelloInfo) (*tls.Config, error) {
	ip := client.Conn.RemoteAddr().String()

	authIPs := viper.GetStringSlice("ip_whitelist")

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
