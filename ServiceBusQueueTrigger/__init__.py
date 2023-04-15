import logging
import psycopg2
import azure.functions as func


def main(msg: func.ServiceBusMessage):
    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)
    # TODO: Get connection to database
    try:
        logging.info('try')
        # TODO: Get notification message and subject from database using the notification_id

        # TODO: Get attendees email and name

        # TODO: Loop through each attendee and send an email with a personalized subject

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        logging.info('finally')
