import vk_api
from vk_api.exceptions import ApiError, AuthError
import mysql.connector as connector
from datetime import datetime
from constants import *


def auth_handler():
    key = input("Enter authentication code: ")
    remember_device = True
    return key, remember_device


def main():

    # Connect to MySQL database
    conn = connector.connect(
        host=host,
        user=user,
        password=db_password,
        database=database
    )
    cur = conn.cursor()

    # Connect to VK API
    try:
        vk_session = vk_api.VkApi(
            login=login,
            password=vk_password,
            app_id=app_id,
            auth_handler=auth_handler,
            token=token
        )
        vk = vk_session.get_api()
    except AuthError as error_message:
        print(error_message)
        return

    offset = 1  # Ignore the pinned post
    for _ in range(11):
        
        try:
            wall_posts = vk.wall.get(
                owner_id=f"-22122222",
                count=50,
                offset=offset
            )
        except ApiError as error_message:
            print(f"VK API error: {error_message}")
            return

        for post in wall_posts['items']:
            post_date = datetime.fromtimestamp(post["date"])
            if not post["attachments"]:
                attachment_type = "repost"
            else:
                attachment_type = post["attachments"][0]["type"]
            text_size = len(post["text"])
            like_count = post['likes']['count']
            comment_count = post['comments']['count']
            repost_count = post['reposts']['count']
            view_count = post["views"]["count"]
            duration = post["attachments"][0]["video"]["duration"] if attachment_type == "video" else None
        
            # Insert data into MySQL table
            cur.execute("""
            INSERT INTO posts 
            (post_date, attachment_type, text_size, like_count, comment_count, repost_count, view_count, duration)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (post_date, attachment_type, text_size, like_count, comment_count, repost_count, view_count, duration)
            )      

        offset += 50
    conn.commit()


if __name__ == "__main__":
    main()
