import base64
import pdfplumber

def basic_auth_token(username, password):
    user_pass = f"{username}:{password}"
    token_bytes = base64.b64encode(user_pass.encode('utf-8'))
    token_str = token_bytes.decode('ascii')
    return f"Basic {token_str}"

def extract_text_from_pdf(pdf_path):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text() or ""
    return text