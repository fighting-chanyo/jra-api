import requests
import sys
import os

# Minimal 1x1 PNG
DUMMY_PNG_BYTES = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82'

def create_dummy_image(filename="dummy.png"):
    with open(filename, "wb") as f:
        f.write(DUMMY_PNG_BYTES)
    print(f"Created dummy image: {filename}")
    return filename

def test_analyze_image(image_path):
    url = "http://localhost:8000/api/analyze/image"
    
    if not os.path.exists(image_path):
        print(f"Error: File {image_path} not found.")
        return

    print(f"Sending {image_path} to {url}...")
    
    try:
        with open(image_path, "rb") as f:
            files = {"file": (os.path.basename(image_path), f, "image/png")}
            response = requests.post(url, files=files)
        
        print(f"Status Code: {response.status_code}")
        try:
            print("Response JSON:")
            print(response.json())
        except:
            print("Response Text:")
            print(response.text)
            
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the server. Is it running?")
        print("Run: uvicorn app.main:app --reload")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        image_path = sys.argv[1]
    else:
        print("No image path provided. Creating a dummy image.")
        image_path = create_dummy_image()
        
    test_analyze_image(image_path)
