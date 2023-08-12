from kiwi import app


class Job:
    def __init__(self, date: str, suffix: str):
        self.date = date
        self.suffix = suffix

    def run(self) -> None:
        print(self.date, self.suffix, app.config['fp'])
