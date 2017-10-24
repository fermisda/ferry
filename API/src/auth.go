package main
import (
"crypto/x509"
"fmt"
//"log"
//"net/http"
"os"
"bytes"
)


// do some stuff to parse certs, verify then, etc.

// we probably need   list of authorized DNs too
func checkAuth () {

fmt.Println("This doesn't do anything yet.")

}
func loadCerts(certs []string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	for _, ca := range certs {
//		log.WithField("file", ca).Info("Loading CA certs")
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
