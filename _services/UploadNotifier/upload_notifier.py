import json
import fastf1
from sqlalchemy.orm import Session
from _repository.engine import postgres
from _repository.repository import EventSessions, Events, Subscriptions
from _services.UploadNotifier.push_sender import send_push_message


class UploadNotifier:

    def _get_subscriptions(self):
        with Session(postgres) as s:
            return s.query(Subscriptions).all()

    def _get_session_data(self, year: str, event: str, session: str):
        return fastf1.get_session(
            year=int(year), gp=int(event), identifier=int(session)
        )

    def send_notification(self, year: str, event: str, session: str):
        subscriptions = self._get_subscriptions()
        session_data = self._get_session_data(year, event, session)
        for subscription in subscriptions:
            send_push_message(
                subscription=json.loads(subscription.subscription),
                title="New session data update",
                description="The data for event "
                + session_data.event.EventName
                + ", session "
                + session_data.name
                + " has been uploaded",
            )
