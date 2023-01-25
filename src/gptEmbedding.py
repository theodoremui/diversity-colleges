import torch
from transformers import GPT2Tokenizer, GPT2Model

# Load the pre-trained model and tokenizer
model = GPT2Model.from_pretrained("gpt2")
tokenizer = GPT2Tokenizer.from_pretrained("gpt2")

# Encode the input text
input_text = "What is the capital of France?"
encoded_input = tokenizer.encode(input_text, return_tensors="pt")

# Get the last hidden states of the model
output = model(encoded_input)
last_hidden_states = output[0]

# Extract the embeddings for the input text
embeddings = last_hidden_states[0][0]

print(embeddings.shape)