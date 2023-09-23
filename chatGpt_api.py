import openai

# Set up the API key
openai.api_key = 'your-api-key'

# Make a call to the API
response = openai.Completion.create(
  engine="text-davinci-003",
  prompt="Once upon a time,",
  temperature=0.6,
  max_tokens=100
)

# Print the API response
print(response.choices[0].text.strip())
