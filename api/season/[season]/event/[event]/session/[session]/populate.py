from http.server import BaseHTTPRequestHandler

from _services.fetcher import store_session_data_to_db


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        path_segments = self.path.split("/")
        season = path_segments[2]
        event = int(path_segments[4])
        session = int(path_segments[6])
        store_session_data_to_db(season, event, session)
        self.send_response(200)
        return
