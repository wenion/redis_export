from redis_om import Migrator, get_redis_connection
from redis_om import Field, JsonModel, EmbeddedJsonModel
from pydantic import NonNegativeInt
from typing import Optional
import csv
import datetime
import json
import re
import os

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


def write_user_event_csv(data, filename):
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


def write_csv(data, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        first_dict = data[0]
        writer.writerow(first_dict.keys())  # Write header row
        for user_event in data:
            writer.writerow(user_event.values())


def get_bookmark(offset, limit):
    page_bookmark = Bookmark.find().page(offset, limit)
    bookmark_dict = []
    for bookmark in page_bookmark:
        editable_bookmark = bookmark.dict()
        editable_bookmark["username"] = split_user(editable_bookmark["user"]["userid"])["username"]
        editable_bookmark.pop("user")
        result = Result.get(editable_bookmark["result"])
        editable_result = result.dict()
        editable_result.pop("pk")
        editable_bookmark.update(editable_result)
        bookmark_dict.append(editable_bookmark)
    return bookmark_dict


def get_user_event(offset, limit):
    page_user_event = UserEvent.find().page(offset, limit)
    user_event_dict = []
    for bookmark in page_user_event:
        editable_bookmark = bookmark.dict()
        editable_bookmark["username"] = split_user(editable_bookmark["userid"])["username"]
        editable_bookmark.pop("userid")
        editable_bookmark["time"] = convert_epoch_milliseconds_to_datetime(editable_bookmark['timestamp'])

        user_event_dict.append(editable_bookmark)
    return user_event_dict


def get_user_role(offset, limit):
    page_user_role = UserRole.find().page(offset, limit)
    user_role_dict = []
    for user_role in page_user_role:
        user_role_dict.append(user_role.dict())
    return user_role_dict


# func -> dict
def write_csv(total, filename, func):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        headered = False
        page_size = 1000
        offset = 0

        while offset < total:
            results = func(offset, page_size)

            if not headered:
                writer.writerow(results[0].keys())  # Write header row
                headered = True

            for result in results:
                writer.writerow(result.values())

            offset += page_size


def write_json(total, filename, func):
    base_name, extension = os.path.splitext(filename)

    page_size = 1000
    offset = 0

    bunch = []

    while offset < total:
        bunch_data = func(offset, page_size) # offset limit
        for item in bunch_data:
            bunch.append(item.dict())
        with open(base_name + "_" + str(offset // page_size + 1) + extension, "w", newline="") as josnfile:
            json.dump(bunch, josnfile)
        bunch.clear()
        offset += page_size



if __name__ == "__main__":
    Migrator().run()

    filename = "export_bookmark.csv"
    total = Bookmark.find().count()
    write_csv(total, filename, get_bookmark)

    filename = "export_user_event.csv"
    total = UserEvent.find().count()
    write_csv(total, filename, get_user_event)

    filename = "export_bookmark.json"
    total = Bookmark.find().count()
    write_json(total, filename, Bookmark.find().page)

    filename = "export_result.json"
    total = Result.find().count()
    write_json(total, filename, Result.find().page)

    filename = "export_user_role.json"
    total = UserRole.find().count()
    write_json(total, filename, UserRole.find().page)

    filename = "export_user_role.csv"
    total = UserEvent.find().count()
    write_csv( total, filename, get_user_role)


    # all_user_event = UserEvent.find().all()

    # all_bookmark = Bookmark.find().all()
    # print("all count", UserEvent.find().count())

    # data = []
    # for bookmark in all_bookmark:
    #     bookmark_dict = bookmark.dict()
    #     userid = bookmark_dict["user"]["userid"]
    #     username = split_user(userid)["username"]
    #     result_detail = Result.find(
    #         Result.pk == bookmark_dict["result"]
    #     ).all()[0].dict()
    #     bookmark_dict["username"] = username
    #     bookmark_dict.pop("user")
    #     result_detail.pop("pk")
    #     bookmark_dict.update(result_detail)
    #     print("\n", bookmark_dict)
    #     data.append(bookmark_dict)


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
    # write_user_event_csv(all_user_event, filename)
    # write_csv(data, filename)