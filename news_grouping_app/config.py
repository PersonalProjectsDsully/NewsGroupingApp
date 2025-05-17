import os

# Use this environment variable to switch the OpenAI model globally.
# For example, to use the gpt-4.1-mini model:
#   export OPENAI_MODEL="gpt-4.1-mini"
#OPENAI_MODEL = os.getenv("OPENAI_MODEL", "o3-mini")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
