#!/usr/bin/env python3
import os.path
from typing import Sequence

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"
SHORTCUT_MIME_TYPE = "application/vnd.google-apps.shortcut"

only_folders = f"(mimeType = '{FOLDER_MIME_TYPE}' or mimeType = '{SHORTCUT_MIME_TYPE}')"

DOMAIN_ALLOWLIST = "hb.edu"

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive"]


class DriveFileNotFound(Exception):
    pass


def authorize():
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if creds is None:
        flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
        creds = flow.run_local_server(port=0)
    elif not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
    # Save the credentials for the next run
    with open("token.json", "w") as token:
        token.write(creds.to_json())
    return creds


def main():
    # dir_to_preserve_id = "0BztiuAHGRdQLaFdPa0VsRk1lYms" # 2399Mentors
    # dir_to_preserve = {
    #     "path": ["2399"],
    #     "file": {"id": "0B2eATzyg5I-5MXNjTDRTeDN2dEk"},
    # }  # 2399
    dir_to_preserve = {
        "path": ["Robotics"],
        "file": {"id": "0B7MR3hbs6jiyRVVfQ2tvUElyQ1k"},
    }
    dir_to_clone_to = {
        "path": ["moved to HB"],
        "file": {"id": "177vF6keSD2SXPRbmPRZwl056d2uje09N"},
    }

    creds = authorize()

    service = build("drive", "v3", credentials=creds)

    known_paths = {}

    def find_by_path(path_segments: Sequence[str], start, extra_q=None):
        cachekey = tuple(start["path"] + path_segments)
        if v := known_paths.get(cachekey):
            return v

        if extra_q is None:
            extra_q = []

        last_parent = start["file"]
        search_for_segment = path_segments[0]
        _path_segments = path_segments[1:]
        # Enumerate the directory tree
        pageToken = None
        while search_for_segment:
            query_parts = [*extra_q]
            if len(_path_segments) > 0:
                query_parts.append(only_folders)

            if last_parent is not None:
                query_parts.append(f"'{last_parent['id']}' in parents")

            results = (
                service.files()
                .list(
                    pageSize=1000,
                    orderBy="name",
                    pageToken=pageToken,
                    fields="nextPageToken, files(id, name, mimeType, owners, shortcutDetails)",
                    q=" and ".join(query_parts) if len(query_parts) > 0 else None,
                )
                .execute()
            )
            for file in results["files"]:
                if file["name"] == search_for_segment:
                    # Found this path segment, save it and search its contents
                    last_parent = file
                    pageToken = None
                    if len(_path_segments) > 0:
                        search_for_segment = _path_segments[0]
                        _path_segments = _path_segments[1:]
                    else:
                        # done, get me outta here
                        search_for_segment = None
                    break  # back to search_for_segment
            else:
                # go to next page if not found
                try:
                    pageToken = results["nextPageToken"]
                except KeyError as e:
                    raise DriveFileNotFound(_path_segments) from e
        known_paths[cachekey] = {
            "path": start["path"] + path_segments,
            "file": last_parent,
        }
        return known_paths[cachekey]

    def walk_tree(from_parent, resolve_shortcuts=True):
        current_parent = from_parent

        folders_to_traverse = []

        pageToken = None
        while current_parent is not None:
            query_parts = [f"'{current_parent['file']['id']}' in parents"]
            results = (
                service.files()
                .list(
                    pageSize=1000,
                    orderBy="name",
                    pageToken=pageToken,
                    fields="nextPageToken, files(id, name, mimeType, owners, shortcutDetails)",
                    q=" and ".join(query_parts) if len(query_parts) > 0 else None,
                )
                .execute()
            )
            for file in results["files"]:
                file_with_path = {
                    "path": [*current_parent["path"], file["name"]],
                    "file": file,
                }
                if resolve_shortcuts and file["mimeType"] == SHORTCUT_MIME_TYPE:
                    # retrieve real file
                    try:
                        file_with_path["file"] = (
                            service.files()
                            .get(
                                fileId=file["shortcutDetails"]["targetId"],
                                fields="id, name, mimeType, owners, shortcutDetails",
                            )
                            .execute()
                        )
                    except HttpError as e:
                        if e.resp.status == 400:
                            print(f"error={e}")
                            errors.append(e)
                        else:
                            breakpoint()
                            print(e)
                    except Exception as e:
                        breakpoint()
                        print(e)
                if file_with_path["file"]["mimeType"] == FOLDER_MIME_TYPE:
                    folders_to_traverse.append(file_with_path)
                else:
                    # Plain file, spit it out
                    yield file_with_path
            # go to next page
            try:
                pageToken = results["nextPageToken"]
            except KeyError:
                pageToken = None
                if len(folders_to_traverse) > 0:
                    current_parent = folders_to_traverse[0]
                    folders_to_traverse = folders_to_traverse[1:]
                else:
                    current_parent = None

    # mkdir -p equivalent
    def ensure_is_folder(path_segments: Sequence[str], start):
        print(f"Ensuring folder {path_segments=}")
        cachekey = tuple(start["path"] + path_segments)
        if v := known_paths.get(cachekey):
            return v

        last_parent = start["file"]
        search_for_segment = path_segments[0]
        _path_segments = path_segments[1:]
        # Enumerate the directory tree
        pageToken = None
        while search_for_segment:
            query_parts = [f"mimeType = '{FOLDER_MIME_TYPE}'"]

            if last_parent is not None:
                query_parts.append(f"'{last_parent['id']}' in parents")

            results = (
                service.files()
                .list(
                    pageSize=1000,
                    orderBy="name",
                    pageToken=pageToken,
                    fields="nextPageToken, files(id, name, mimeType, owners, shortcutDetails)",
                    q=" and ".join(query_parts) if len(query_parts) > 0 else None,
                )
                .execute()
            )
            for file in results["files"]:
                if file["mimeType"] == FOLDER_MIME_TYPE:
                    if file["name"] == search_for_segment:
                        # Found this path segment, save it and search its contents
                        last_parent = file
                        pageToken = None
                        if len(_path_segments) > 0:
                            search_for_segment = _path_segments[0]
                            _path_segments = _path_segments[1:]
                        else:
                            # done, get me outta here
                            search_for_segment = None
                        break  # back to search_for_segment
            else:
                # go to next page if not found
                try:
                    pageToken = results["nextPageToken"]
                except KeyError:
                    # Create path
                    last_parent = (
                        service.files()
                        .create(
                            body={
                                "mimeType": FOLDER_MIME_TYPE,
                                "name": search_for_segment,
                                "parents": [last_parent["id"]],
                            },
                            fields="id, name, mimeType",
                        )
                        .execute()
                    )
                    pageToken = None
                    if len(_path_segments) > 0:
                        search_for_segment = _path_segments[0]
                        _path_segments = _path_segments[1:]
                    else:
                        # done, get me outta here
                        search_for_segment = None
        known_paths[cachekey] = {
            "path": start["path"] + path_segments,
            "file": last_parent,
        }
        return known_paths[cachekey]

    # try:
    #     dest_dir = find_by_path(dir_to_preserve["path"], start=dir_to_clone_to)
    #     for file_with_path in walk_tree(dest_dir, resolve_shortcuts=False):
    #         if file_with_path["file"]["mimeType"] == SHORTCUT_MIME_TYPE:
    #             service.files().delete(fileId=file_with_path["file"]["id"]).execute()
    #             print(f"deleted={file_with_path}")
    # except DriveFileNotFound:
    #     print("could not find dest_dir, will be created")

    errors = []

    for file_with_path in walk_tree(dir_to_preserve):
        owner_ok = False
        for owner in file_with_path["file"]["owners"]:
            owner_domain = owner["emailAddress"].rsplit("@")[-1]
            if owner_domain in DOMAIN_ALLOWLIST:
                owner_ok = True
                break
        if not owner_ok:
            print(f"{file_with_path=}")
            dest_path = file_with_path["path"]
            print(f"{dest_path=}")
            try:
                found = find_by_path(
                    dest_path,
                    start=dir_to_clone_to,
                )
                print(f"{found=}")
                if found["file"]["mimeType"] == SHORTCUT_MIME_TYPE:
                    breakpoint()
            except DriveFileNotFound:
                file_dest = ensure_is_folder(
                    dest_path[:-1],
                    start=dir_to_clone_to,
                )
                print(f"{file_dest=}")
                try:
                    copied = (
                        service.files()
                        .copy(
                            fileId=file_with_path["file"]["id"],
                            body={
                                "parents": [file_dest["file"]["id"]],
                                "name": file_with_path["file"]["name"],
                            },
                            fields="id, name, mimeType",
                        )
                        .execute()
                    )
                    print(f"{copied=}")
                except HttpError as e:
                    if e.resp.status == 400:
                        print(f"error={e}")
                        errors.append(e)
                    else:
                        breakpoint()
                        print(e)
                except Exception as e:
                    breakpoint()
                    print(e)
    return


if __name__ == "__main__":
    main()
