from modules.jasypt4py import StandardPBEStringEncryptor

ENC_TYPE_SHA256='PBEWITHSHA256AND256BITAES-CBC'
ENC_KEY = 'shbkey'

# crypto test
def encrypt(text, enc_type=ENC_TYPE_SHA256, pwd=ENC_KEY):
    
    cryptor = StandardPBEStringEncryptor(enc_type)

    return cryptor.encrypt(pwd, text)

# crypto test
def decrypt(text, enc_type=ENC_TYPE_SHA256, pwd=ENC_KEY):
    cryptor = StandardPBEStringEncryptor(enc_type)
    
    return cryptor.decrypt(pwd, text)