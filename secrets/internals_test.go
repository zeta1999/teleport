package secrets

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	settings    = Settings{"TESTAPP", "TESTAPP_SECRET_KEY", ""}
	sampleKey   = "USER_TOKEN"
	sampleValue = "SAMPLE1joxLCJTYWx0Ijoie"
)

func TestVariableEncryptAndDecrypt(t *testing.T) {
	os.Setenv(settings.KeyEnvVariable, "SAMPLExU1lBMkZtS2czMUR3")
	defer os.Unsetenv(settings.KeyEnvVariable)

	header := header{}
	header.resetSalt()
	salt := header.Salt
	key, err := deriveKey(settings.KeyEnvVariable, salt)
	assert.NoError(t, err)

	variable := Variable{sampleKey, sampleValue}
	encoded, err := variable.encryptValueAndEncode(key)
	assert.NoError(t, err)

	variableRecovered, err := decodeAndDecryptVariable(key, encoded)
	assert.NoError(t, err)

	assert.Equal(t, sampleKey, variableRecovered.Key)
	assert.Equal(t, sampleValue, variableRecovered.Value)
}

func TestDoesNotReuseNonce(t *testing.T) {
	os.Setenv(settings.KeyEnvVariable, "SAMPLExU1lBMkZtS2czMUR3")
	defer os.Unsetenv(settings.KeyEnvVariable)

	header := header{}
	header.resetSalt()
	salt := header.Salt
	key, err := deriveKey(settings.KeyEnvVariable, salt)
	assert.NoError(t, err)

	ciphertext1, err := encrypt(key, sampleValue)
	assert.NoError(t, err)
	ciphertext2, err := encrypt(key, sampleValue)
	assert.NoError(t, err)

	assert.NotEqual(t, ciphertext2, ciphertext1)
}

func TestResetsSaltOnEachUpdate(t *testing.T) {
	withSecretsFile(t, func(settings Settings) {
		header, err := readHeader(settings)
		assert.NoError(t, err)
		salt1 := header.Salt

		err = UpdateSecret(settings, sampleKey, sampleValue)
		assert.NoError(t, err)

		header, err = readHeader(settings)
		assert.NoError(t, err)
		salt2 := header.Salt

		assert.NotEqual(t, salt2, salt1)
	})
}

func withSecretsFile(t *testing.T, testfn func(Settings)) {
	tmpFile, err := ioutil.TempFile(os.TempDir(), "secrets")
	if err != nil {
		t.Fatal("cannot create temporary file:", err)
	}
	defer os.Remove(tmpFile.Name())

	os.Setenv(settings.KeyEnvVariable, "SAMPLExU1lBMkZtS2czMUR3")
	defer os.Unsetenv(settings.KeyEnvVariable)

	settings.SecretsFile = tmpFile.Name()
	InitializeSecretsFile(settings)

	testfn(settings)
}
