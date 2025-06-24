from http.server import BaseHTTPRequestHandler

from _services.fetcher import init_year_data


class handler(BaseHTTPRequestHandler):

    def do_GET(self):
        season = self.path.split("/")[2]
        init_year_data(season)
        self.send_response(200)
        return
