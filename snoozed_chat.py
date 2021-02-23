# query the data from DB (current_time <= snoozed_time).
# if record exists then reopen the records by looping the channels.
import logging
import os
import sys
import re

import pymysql
from bson import ObjectId

from app.pubnub_init import publish_message, batch_history
import constants

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(asctime)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("logger")

try:
    logger.debug("setting up connection with DB")
    dbUrl = os.getenv("DATABASE_URI", "mysql+pymysql://root:root@localhost:3306/dbcloud")

    m = re.search("^([^\:]*)://([^@]*)@([^:]*):([^\/]*)\/(.*)$", dbUrl)

    (const_url, username_password, host, port, dbname) = m.groups()
    username, password = username_password.split(":")
    port = int(port)
    db = pymysql.connect(host, username, password, dbname, port)
    cursor = db.cursor()
except Exception as ex:
    logger.debug(ex)
    raise ex


# function, will reopen snoozed chat
def reopen_snoozed_chat():
    logger.debug("re-opening snoozed chat...")
    try:
        pb_channels = get_snoozed_channel()
        if not len(pb_channels):
            logger.error("no channel found for reopening the snoozed chat...")
        else:
            pb_channel_ids = []
            for pb_channel in pb_channels:
                if pb_channel:
                    pb_channel_id = pb_channel[0]
                    pb_channel_ids.append(pb_channel_id)
                    company_id = pb_channel[1]
                    # re-opening all the closed chats...
                    send_reopen_notification(company_id, pb_channel)

            # reset snoozed chat in DB
            if len(pb_channel_ids) > 0:
                reset_snoozed_chat(pb_channel_ids)
    except Exception as e:
        logger.exception(e)
        raise e


# get snoozed channel as per current time
def get_snoozed_channel():
    logger.debug("getting snoozed chat channels")
    try:
        sql = "SELECT * FROM pb_channel where snoozed_till <= now()"
        cursor.execute(sql)
        pb_channels = cursor.fetchall()
        return pb_channels
    except Exception as e:
        logger.exception(e)
        raise e


# send reopen notification channel event
def send_reopen_notification(company_id, pb_channel):
    logger.debug("pub nub reopen notification....")
    last_message = None
    customer_uuid = pb_channel[13]
    chat_enable = pb_channel[9]
    history = batch_history([pb_channel[4]]).get(pb_channel[4])
    if history:
        last_history_item = history[-1]
        last_message = {'message': last_history_item.message, 'timetoken': int(last_history_item.timetoken)}

    if chat_enable:
        cobrowse_url = constants.COBROWSE_URL + customer_uuid
    else:
        cobrowse_url = None

    # channel reopen, send channel to ui for subscription
    publish_message('ch-events-comp-' + str(company_id),
                    {'type': 'state', 'previous': "snoozed", 'current': "opened", 'triggered_by': None,
                     'channel_id': pb_channel[0], 'client_id': str(ObjectId(pb_channel[7])), 'channel': pb_channel[4],
                     'name': pb_channel[10], 'number': pb_channel[11], 'mail': pb_channel[12],
                     'uuid': pb_channel[13], 'cobrowse_url': cobrowse_url,
                     'chat_enabled': pb_channel[9], 'fb_client_id': pb_channel[15],
                     'assignee_type': pb_channel[5], 'assignee_id': pb_channel[6],
                     'team_id': pb_channel[3], 'last_message': last_message,
                     'ticket_id': pb_channel[2]}, should_store=False)


def reset_snoozed_chat(pubnub_ids):
    logger.debug("getting snoozed chat channels")
    try:
        sql = "UPDATE pb_channel SET snoozed_till = null WHERE id IN %(pubnub_ids)s"
        cursor.execute(sql, {"pubnub_ids": pubnub_ids})
        db.commit()
        return
    except Exception as e:
        logger.exception(e)
        raise e


reopen_snoozed_chat()
