import tornado.ioloop
import tornado.web
import json
import psycopg2

conn = psycopg2.connect(
    "host=%s dbname=%s user=%s password=%s" %
    ("localhost", "roverdb", "rover", "woof")
)
cursor = conn.cursor()

class SitterHandler(tornado.web.RequestHandler):
    def get(self):
        sitters = []
        cursor.execute(
            """
            select
                a.name,
                a.image_link,
                round(b.score, 2)::text
            from sitters a
            join sitter_scores b
                on a.id = b.id
            order by b.score desc
            """
        )

        for row in cursor:
            sitters.append({
                "name": row[0],
                "url": row[1],
                "score": row[2]
            })

        output = { "sitters": sitters }
        self.write(json.dumps(output))

##################################################

def make_app():
    return tornado.web.Application([
        (r"/sitters", SitterHandler),
    ])

if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
