import hashlib
sample_text = "My name is Ayub and I love Python".encode('utf-8')
sample_output = hashlib.md5(sample_text).hexdigest()
print(sample_text)
#sample_text: b'My name is Ayub and I love Python'
print(sample_output)
# sample_ouput: adf6af4969819ab07fa8349415707a67
