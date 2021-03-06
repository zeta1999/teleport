package secrets

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"os"
	"sort"
	"strings"

	"golang.org/x/crypto/pbkdf2"
)

// Version contains the current encryption process version
var Version = 2

// Settings contains application-specific settings
type Settings struct {
	AppName        string
	KeyEnvVariable string
	SecretsFile    string
}

// Body is a slice of Variables
type Body []Variable

// Variable represents a single, unecrypted key-value pair
type Variable struct {
	Key   string
	Value string

	encryptedValue string
}

type header struct {
	Version int
	Salt    string
}

var nonceSize = 12

var applicationSalt = "3p7cGstorIAZUeSOiOf60yWeYKi2wPpyvfAjMnH+dbM="

// InitializeSecretsFile creates an empty secrets file
func InitializeSecretsFile(settings Settings) error {
	header := header{Version, ""}
	header.resetSalt()
	return writeSecretsFile(settings, header, make(Body, 0))
}

// GenerateSecretKey generates a random string for use as the Secret Key
func GenerateSecretKey() (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyz" +
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	secret := make([]byte, 64)
	for i := range secret {
		randint, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return "", err
		}
		secret[i] = charset[randint.Int64()]
	}

	return string(secret), nil
}

// UpdateSecret adds updates the secret corresponding to key with the given value
func UpdateSecret(settings Settings, key string, value string) error {
	header, err := readHeader(settings)
	if err != nil {
		return err
	}

	body, err := ReadSecretsFile(settings)
	if err != nil {
		return err
	}

	body = append(body, Variable{Key: key, Value: value})

	err = writeSecretsFile(settings, header, body)
	if err != nil {
		return err
	}

	return nil
}

// DeleteSecret deletes the secret with the given key
func DeleteSecret(settings Settings, key string) error {
	header, err := readHeader(settings)
	if err != nil {
		return err
	}

	body, err := ReadSecretsFile(settings)
	if err != nil {
		return err
	}

	index := -1
	for i, variable := range body {
		if variable.Key == key {
			index = i
			break
		}
	}
	if index == -1 {
		return fmt.Errorf("%s key not found in secrets file", key)
	}

	newBody := body[:index]
	newBody = append(newBody, body[index+1:]...)

	err = writeSecretsFile(settings, header, newBody)
	if err != nil {
		return err
	}

	return nil
}

// ReadSecretsFile returns the unecrypted body from a secrets file
func ReadSecretsFile(settings Settings) (body Body, err error) {
	osfile, err := os.Open(settings.SecretsFile)
	if err != nil {
		return
	}
	scanner := bufio.NewScanner(osfile)
	header := header{}
	headerString := ""
	inHeader := false
	inBody := false
	key := make([]byte, 0)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "-- HEADER --" {
			inHeader = true
		} else if line == "-- BODY --" {
			inHeader = false
			inBody = true

			unmarshalledJSON, headererr := base64.StdEncoding.DecodeString(headerString)
			if headererr != nil {
				return body, headererr
			}

			headererr = json.Unmarshal(unmarshalledJSON, &header)
			if headererr != nil {
				return body, headererr
			}

			key, headererr = deriveKey(settings, header)
			if headererr != nil {
				return body, headererr
			}
		} else if inHeader {
			headerString += line
		} else if inBody && line != "" {
			var variable Variable
			variable, err = decodeAndDecryptVariable(key, line)
			if err != nil {
				return
			}
			body = append(body, variable)
		}
	}

	return body, nil
}

// EncryptionVersion returns the encryption version used in the secrets file
func EncryptionVersion(settings Settings) (int, error) {
	header, err := readHeader(settings)
	if err != nil {
		return -1, err
	}

	return header.Version, nil
}

func readHeader(settings Settings) (header header, err error) {
	osfile, err := os.Open(settings.SecretsFile)
	if err != nil {
		return
	}
	scanner := bufio.NewScanner(osfile)
	headerString := ""
	inHeader := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "-- HEADER --" {
			inHeader = true
		} else if line == "-- BODY --" {
			break
		} else if inHeader {
			headerString += line
		}
	}

	unmarshalledJSON, err := base64.StdEncoding.DecodeString(headerString)
	if err != nil {
		return
	}

	err = json.Unmarshal(unmarshalledJSON, &header)

	return
}

func (h *header) resetSalt() error {
	salt := make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, salt)
	if err != nil {
		return err
	}
	h.Salt = base64.StdEncoding.EncodeToString(salt)

	return nil
}

func (b *Body) encryptAndEncode(key []byte) (string, error) {
	keys := make([]string, 0)
	encryptedValues := make(map[string]string)
	for _, variable := range *b {
		encrypted, err := variable.encryptValueAndEncode(key)
		if err != nil {
			return "", err
		}

		for _, key := range keys {
			if variable.Key == key {
				encryptedValues[variable.Key] = encrypted
			}
		}

		if _, ok := encryptedValues[variable.Key]; !ok {
			keys = append(keys, variable.Key)
			encryptedValues[variable.Key] = encrypted
		}
	}

	sort.Strings(keys)

	lines := make([]string, 0)
	for _, key := range keys {
		lines = append(lines, encryptedValues[key])
	}

	return strings.Join(lines, "\n"), nil
}

func (v *Variable) ToEnvFormat() string {
	return fmt.Sprintf("%s=%s", v.Key, v.Value)
}

func (v *Variable) encryptValueAndEncode(key []byte) (string, error) {
	secretData := make([]string, 2)
	if v.encryptedValue != "" {
		secretData = []string{v.Key, v.encryptedValue}
	} else {
		encryptedValue, err := encrypt(key, v.Value)
		if err != nil {
			return "", err
		}

		secretData = []string{v.Key, encryptedValue}
	}

	secretBytes, err := json.Marshal(secretData)
	if err != nil {
		return "", err
	}

	return string(secretBytes), nil
}

func decodeAndDecryptVariable(key []byte, encoded string) (variable Variable, err error) {
	secretData := make([]string, 2)

	err = json.Unmarshal([]byte(encoded), &secretData)
	if err != nil {
		return
	}

	variable.Key = secretData[0]
	variable.Value, err = decrypt(key, secretData[1])
	variable.encryptedValue = secretData[1]

	return
}

func writeSecretsFile(settings Settings, header header, body Body) error {
	if len(header.Salt) < 32 {
		return errors.New("secrets file not initialized, run `secrets init` first")
	}

	if header.Version < Version {
		// update header version
		header.Version = Version

		// reset cached encryped values
		for i := range body {
			body[i].encryptedValue = ""
		}
	}

	headerBytes, err := json.Marshal(&header)
	headerString := strings.Join(split(base64.StdEncoding.EncodeToString(headerBytes), 64), "\n")

	contents := fmt.Sprintf(`# THIS FILE WAS AUTO-GENERATED BY %s
# DO NOT MODIFY THIS FILE
#
# This file can be safely saved in version control or other public locations.
# Store your %s in a secure place, never in version control.
-- HEADER --
%s
-- BODY --
`, settings.AppName, settings.KeyEnvVariable, headerString)

	key, err := deriveKey(settings, header)
	if err != nil {
		return err
	}

	bodyString, err := body.encryptAndEncode(key)
	if err != nil {
		return err
	}

	contents += bodyString
	contents += "\n"

	err = ioutil.WriteFile(settings.SecretsFile, []byte(contents), 0644)
	return err
}

func encrypt(key []byte, plaintext string) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, nonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	ciphertext := aesgcm.Seal(nil, nonce, []byte(plaintext), nil)
	ciphertext = append(nonce, ciphertext...)

	encodedCiphertext := base64.StdEncoding.EncodeToString(ciphertext)
	return encodedCiphertext, nil
}

func decrypt(key []byte, ciphertextString string) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextString)
	if err != nil {
		return "", err
	}
	nonce := ciphertext[:nonceSize]
	ciphertext = ciphertext[nonceSize:]

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	plaintext, err := aesgcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("%s (verify your secret key ENV variable)", err)
	}

	return string(plaintext), nil
}

func deriveKey(settings Settings, header header) ([]byte, error) {
	secret, found := os.LookupEnv(settings.KeyEnvVariable)
	if !found {
		return []byte{}, fmt.Errorf("%s not set", settings.KeyEnvVariable)
	}

	salt, err := base64.StdEncoding.DecodeString(header.Salt)
	if err != nil {
		return []byte{}, err
	}

	if header.Version >= 2 {
		salt = append(salt, applicationSalt...)
	}

	key := pbkdf2.Key([]byte(secret), salt, 64_000, 32, sha512.New)

	return key, nil
}

func split(s string, size int) []string {
	ss := make([]string, 0, len(s)/size+1)
	for len(s) > 0 {
		if len(s) < size {
			size = len(s)
		}
		ss, s = append(ss, s[:size]), s[size:]

	}
	return ss
}
