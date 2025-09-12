CLIENT_ID="REDACTED"
CLIENT_SECRET="REDACTED"
google_fit_refresh_token="REDACTED"

REFRESH = "REDACTED"

import requests
import urllib.parse

REDIRECT_URI = "http://localhost:8080/oauth2callback"  # Must match one of the redirect URIs in your OAuth client

# Scopes for Google Fit (add/remove as needed)
import requests
import urllib.parse
import time
import json
import os

# Replace with your Google OAuth credentials
CLIENT_ID="REDACTED"
CLIENT_SECRET="REDACTED"

# Fitness scopes
SCOPES = [
    "https://www.googleapis.com/auth/fitness.activity.read",
    "https://www.googleapis.com/auth/fitness.heart_rate.read",
    "https://www.googleapis.com/auth/fitness.body.read",
    "https://www.googleapis.com/auth/fitness.location.read"
]

TOKEN_FILE = "google_fit_tokens.json"
TOKEN_URL = "https://oauth2.googleapis.com/token"

def build_auth_url():
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": " ".join(SCOPES),
        "access_type": "offline",
        "prompt": "consent"
    }
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)

def exchange_code_for_tokens(code):
    data = {
        "code": code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code"
    }
    response = requests.post(TOKEN_URL, data=data)
    response.raise_for_status()
    tokens = response.json()
    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens, f, indent=2)
    return tokens

def refresh_access_token(refresh_token):
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }
    response = requests.post(TOKEN_URL, data=data)
    response.raise_for_status()
    tokens = response.json()
    tokens["refresh_token"] = refresh_token  # keep original refresh token
    with open(TOKEN_FILE, "w") as f:
        json.dump(tokens, f, indent=2)
    return tokens

def get_tokens():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as f:
            tokens = json.load(f)
        if "refresh_token" in tokens:
            return refresh_access_token(tokens["refresh_token"])
    # No token file yet → start OAuth flow
    print("Go to this URL and authorize access:\n")
    print(build_auth_url())
    code = input("\nPaste the 'code' from redirect URL: ").strip()
    return exchange_code_for_tokens(code)

def fetch_fitness_data(access_token):
    url = "https://www.googleapis.com/fitness/v1/users/me/dataset:aggregate"
    headers = {"Authorization": f"Bearer {access_token}"}
    end_time = int(time.time() * 1000)
    start_time = end_time - 24 * 60 * 60 * 1000  # last 24 hours

    body = {
        "aggregateBy": [
            {"dataTypeName": "com.google.step_count.delta"},
            {"dataTypeName": "com.google.heart_rate.bpm"},
            {"dataTypeName": "com.google.calories.expended"},
            {"dataTypeName": "com.google.distance.delta"}
        ],
        "bucketByTime": {"durationMillis": 86400000},
        "startTimeMillis": start_time,
        "endTimeMillis": end_time
    }

    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    return response.json()

if __name__ == "__main__":
    tokens = get_tokens()
    access_token = tokens["access_token"]

    print("\n✅ Access token ready, fetching fitness data...")
    data = fetch_fitness_data(access_token)

    print("\n=== Google Fit Data (last 24h) ===")
    print(json.dumps(data, indent=2))
