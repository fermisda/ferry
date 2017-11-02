package main
import (
"crypto/x509"
"fmt"
"log"
"net/http"
"os"
"bytes"
"bufio"
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

func authorize( req *http.Request, authDNs []string ) (bool, string) {
	// authorization should fail by default
	authorized := false

	//string to build the full DN
	certDN := ""
	for _, presCert := range req.TLS.PeerCertificates {
		
		for _, certnames := range presCert.Subject.Names {
			// check the encoding type and set the value if it matches what we expect for "our" certs and proxies
			switch certnames.Type.String() {
			case "0.9.2342.19200300.100.1.25":  // Domain Component or "DC"
				certDN = certDN + "/DC=" + certnames.Value.(string)
			case "2.5.4.6": // Country or "C"
				certDN = certDN + "/C=" + certnames.Value.(string)
			case "2.5.4.10": // Organization or "O"
				certDN = certDN + "/O=" + certnames.Value.(string)
			case "2.5.4.11": // Organizational Unit or "OU"
				certDN = certDN + "/OU=" + certnames.Value.(string)
			case "2.5.4.3": // Common Name or "CN"
				certDN = certDN + "/CN=" + certnames.Value.(string)	
			default: //something we don't expect
				certDN = certDN + "/UNKNOWN="  + certnames.Value.(string)
			}
		}
		for _, authDN := range authDNs {
			if authDN == certDN {
				authorized = true
				log.Printf("Cert matches authorized DN %s.",certDN)
			}
		}
	}
	if certDN != "" {
		return authorized, certDN
	} else {
		return authorized, "No CN found or no cert presented."	
	}
	
}
