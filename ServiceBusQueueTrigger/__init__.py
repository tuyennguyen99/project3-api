import logging
import psycopg2
import azure.functions as func
import os
import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):
    notification_id = msg.get_body().decode('utf-8')
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)
    # TODO: Get connection to database
    try:
        url = os.environ['DbConnection']
        strs = url.replace('\'', '').split(' ')
        userTmpl = 'user='
        dbNameTmpl = 'dbname='
        hostTmpl = 'host='
        portTmpl = 'port='
        pwdTmpl = 'password='

        for s in strs:
            if userTmpl in s:
                user = s.replace(userTmpl, '')
            if dbNameTmpl in s:
                dbname = s.replace(dbNameTmpl, '')
            if hostTmpl in s:
                host = s.replace(hostTmpl, '')
            if portTmpl in s:
                port = s.replace(portTmpl, '')
            if pwdTmpl in s:
                password = s.replace(pwdTmpl, '')
        connection = psycopg2.connect(database = dbname, 
                        user = user, 
                        host= host,
                        password = password,
                        port = port)
        logging.info('try')
        cursor = connection.cursor()
        getNotificationsQuery = 'SELECT id, status, message, submitted_date, completed_date, subject FROM public.notification WHERE id = ' + notification_id
        cursor.execute(getNotificationsQuery)
        notification = cursor.fetchone()
        notification_message = notification[2]
        notification_subject = notification[3]

        getAttendeesQuery = 'SELECT id, first_name, last_name, conference_id, job_position, email, company, city, state, interests, submitted_date, comments FROM public.attendee;'
        cursor.execute(getAttendeesQuery)
        attendees = cursor.fetchall()
        count = 0
        for attendee in attendees:
            count += 1
            first_name = attendee[1]
            email = attendee[5]
            subject = '{}: {}'.format(first_name, notification_subject)
            send_email(email, subject, notification_message)
            logging.info('Email has been sent to {} for attendee {}'.format(email, first_name))
        
        # TODO: Get notification message and subject from database using the notification_id

        # TODO: Get attendees email and name

        # TODO: Loop through each attendee and send an email with a personalized subject

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        status = 'Notified {attendeeCount} attendees'.format(attendeeCount = str(count))
        cursor.execute('UPDATE public.notification SET status=%s, completed_date=%s WHERE id = %s', (status, datetime.datetime.now(), notification_id))

        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        logging.info('Closing connection')
        connection.close()

def send_email(email, subject, body):
    send_grid_api_key = str(os.environ['SENDGRID_API_KEY'])
    if send_grid_api_key is None or send_grid_api_key == '':
        return
    message = Mail(
            from_email=os.environ['ADMIN_EMAIL_ADDRESS'],
            to_emails=email,
            subject=subject,
            plain_text_content=body)

    sg = SendGridAPIClient(send_grid_api_key)
    sg.send(message)
        
