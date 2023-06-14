from redis_om import Migrator, get_redis_connection
from redis_om import Field, JsonModel, EmbeddedJsonModel
from pydantic import NonNegativeInt
from typing import Optional
import csv
import datetime
import re

class UserRole(EmbeddedJsonModel):
    class Meta:
        global_key_prefix = 'h'
        model_key_prefix = 'UserRole'
    userid: str = Field(index=True)
    faculty: str = Field(index=True)
    teaching_role: str = Field(index=True)
    teaching_unit: str = Field(index=True)
    joined_year: NonNegativeInt = Field(index=True)
    years_of_experience: NonNegativeInt = Field(index=True)
    expert: NonNegativeInt = Field(index=True)


class Result(JsonModel):
    class Meta:
        global_key_prefix = 'h'
        model_key_prefix = 'Result'
    title: str = Field(index=True)
    url: str = Field(index=True)
    summary: Optional[str] #= Field(index=True, full_text_search=True, default="")
    highlights: Optional[str] #= Field(index=True, full_text_search=True, default="")


class Bookmark(JsonModel):
    class Meta:
        global_key_prefix = 'h'
        model_key_prefix = 'Bookmark'
    query: str = Field(index=True, full_text_search=True)
    user: UserRole                      # UserRole pk
    result: str = Field(index=True)     # Result pk
    deleted: int = Field(index=True, default=0)


class UserEvent(JsonModel):
    class Meta:
        global_key_prefix = 'h'
        model_key_prefix = 'UserEvent'
    event_type: str = Field(index=True, full_text_search=True)
    timestamp: int = Field(index=True)
    tag_name: str = Field(index=True)     # Result pk
    text_content: str = Field(index=True)
    base_url: str = Field(index=True)
    userid: str = Field(index=True)


def convert_epoch_milliseconds_to_datetime(epoch_milliseconds):
    # Convert epoch with milliseconds to datetime object
    dt = datetime.datetime.fromtimestamp(epoch_milliseconds / 1000.0)

    # Format datetime as a string
    formatted_datetime = dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    return formatted_datetime


def split_user(userid):
    match = re.match(r"^acct:([^@]+)@(.*)$", userid)
    if match:
        return {"username": match.groups()[0], "domain": match.groups()[1]}
    return {"username": "", "domain": ""}


def write_csv(data, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        first_dict = data[0].dict()
        first_dict["time"] = convert_epoch_milliseconds_to_datetime(first_dict['timestamp'])
        first_dict["user"] = split_user(first_dict['userid'])["username"]
        first_dict.pop("userid")
        writer.writerow(first_dict.keys())  # Write header row
        for user_event in data:
            user_event_dict = user_event.dict()
            user_event_dict["time"] = convert_epoch_milliseconds_to_datetime(user_event_dict['timestamp'])
            user_event_dict["user"] = split_user(user_event_dict['userid'])["username"]
            user_event_dict.pop("userid")
            writer.writerow(user_event_dict.values())

if __name__ == "__main__":
    Migrator().run()

    filename = "export_user_event.csv"
    all_user_event = UserEvent.find().all()

    # conditional export
    #
    # e.g. export all tag_name= "SELECT"
    # tag_select_user_event = UserEvent.find(
    #     UserEvent.event_type == "SELECT"
    # ).all()
    #
    # e.g. export all admin user events
    # all_user_event = UserEvent.find(
    #     UserEvent.userid == "acct:admin@localhost"
    # ).all()

    # for user_event in all_user_event:
    #     user_event_dict = user_event.dict()
    #     print(convert_epoch_milliseconds_to_datetime(user_event_dict['timestamp']))
    write_csv(all_user_event, filename)