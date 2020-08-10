package secrets

import (
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

	variable := Variable{Key: sampleKey, Value: sampleValue}
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
