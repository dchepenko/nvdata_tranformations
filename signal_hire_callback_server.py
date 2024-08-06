from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os

class CallbackHandler(BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        json_data = json.loads(post_data.decode('utf-8'))[0]

        # Save the received JSON data to a file
        candidate_info = json_data.get('candidate', {})
        full_name = candidate_info.get('fullName', 'unknown')
        slug_name = full_name.replace(' ', '_').lower()
        file_path = f'./json/{slug_name}_signalhire.json'

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, 'w') as file:
            json.dump(json_data, file)
        print(f"Saved callback data to {file_path}")
        # Respond to SignalHire with a simple message
        self._set_response()
        response_message = json.dumps({'message': 'Callback received'})
        self.wfile.write(response_message.encode('utf-8'))

def run(server_class=HTTPServer, handler_class=CallbackHandler, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting httpd server on port {port}...')
    httpd.serve_forever()

if __name__ == '__main__':
    run(port=5000)