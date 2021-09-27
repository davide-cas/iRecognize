# pip install pyTelegramBotAPI
import telebot, requests, validators, csv, time, os, re, os.path, threading
from datetime import datetime

TOKEN = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
bot = telebot.TeleBot(token = TOKEN, threaded = False)

# The message received is a text
@bot.message_handler(content_types=["text"])
def message_received(message):
    msg = message.text
    
    # if the text sent is an url
    if validators.url(msg) == True:
        chat_id = message.from_user.id
        user_name = str(message.from_user.username)
        if user_name == "None": user_name = str(message.from_user.first_name)
        file_name = msg.split("/")[-1]
        
        extensions = [".jpg", ".jpeg", ".png", ".JPG", ".JPEG", ".PNG", "img?src"]
        jpg_ext = [".jpg", ".JPG", "img?src"]
        jpeg_ext = [".jpeg", ".JPEG"]
        png_ext = [".png", ".PNG"]
        
        # using list comprehension
        # checking if string contains list element
        res = bool([ext for ext in extensions if(ext in file_name)])
        jpg_res = bool([ext for ext in jpg_ext if(ext in file_name)])
        jpeg_res = bool([ext for ext in jpeg_ext if(ext in file_name)])
        png_res = bool([ext for ext in png_ext if(ext in file_name)])
        
        # if the file format is ".jpg", ".jpeg" or ".png"
        if res == True:
            now = datetime.now()
            dt_string = str(now.strftime("%H-%M-%S-%f"))
            
            # file format = ".jpg"
            if jpg_res == True: file_name = str(chat_id) + "_" + dt_string + ".jpg"
            
            # file format = ".jpeg"
            elif jpeg_res == True: file_name = str(chat_id) + "_" + dt_string + ".jpeg"
            
            # file format = ".png"
            elif png_res == True: file_name = str(chat_id) + "_" + dt_string + ".png"
            
            img = requests.get(msg)
            open(f"/home/davidecas/iRecognize/logstash/csv/photos/{user_name}_{file_name}", 'wb').write(img.content)
            csv_upload(user_name, file_name)
            bot.send_message(chat_id = message.from_user.id, text = "Photo successfully saved!")
            time.sleep(2)
            bot.send_message(chat_id = message.from_user.id, text = "I am detecting, just a second! 🤓")
        
        else:
            bot.send_message(chat_id = message.from_user.id, text = "File format not supported. Please, retry sending a photo 😢")
    
    # if the text sent is a simple text
    else:
        txt = msg.lower()
        
        if "hello" in txt or "hi" in txt: bot.send_message(chat_id = message.from_user.id, text = "Hello " + message.from_user.first_name + "! 😊")
        elif "/start" in txt: bot.send_message(chat_id = message.from_user.id, text = "Welcome " + message.from_user.first_name + "! I am iRecognize bot 🤓\nLet's send me photos, I'll do my best to detect them 😊")
        else: bot.send_message(chat_id = message.from_user.id, text = "Oops! I didn't catch what you mean 🤦🏻‍♂️")

# The message received is a photo
@bot.message_handler(content_types=["photo"])
def message_received(message):
    file_id = message.photo[2].file_id
    chat_id = message.from_user.id
    user_name = str(message.from_user.username)
    if user_name == "None": user_name = str(message.from_user.first_name)
    file_url = bot.get_file_url(file_id)
    
    now = datetime.now()
    dt_string = str(now.strftime("%H-%M-%S-%f")) + ".jpg"

    file_name = str(chat_id) + "_" + dt_string
    
    img = requests.get(file_url)
    open(f"/home/davidecas/iRecognize/logstash/csv/photos/{user_name}_{file_name}", 'wb').write(img.content)
    csv_upload(user_name, file_name)
    bot.send_message(chat_id = message.from_user.id, text = "Photo successfully saved!")
    time.sleep(2)
    bot.send_message(chat_id = message.from_user.id, text = "I am detecting, just a second! 🤓")
    
# The message received is a video
@bot.message_handler(content_types=["video"])
def message_received(message):
    bot.send_message(chat_id = message.from_user.id, text = "Oops! I didn't catch what you mean 🤦🏻‍♂️\nPlease, send me a photo!")

# Checking if there are updates in a specific directory
def directory_checker():
    directory = "/home/davidecas/iRecognize/logstash/csv/detected_photos/"
    before = dict ([(f, None) for f in os.listdir(directory)])
    
    while 1:
        time.sleep(1.25)

        after = dict ([(f, None) for f in os.listdir(directory)])
        added = [f for f in after if not f in before]
        
        if added:
            new_file = added[0]
            
            # There are no predictions
            if "EMPTY" in new_file:
                chat_id = new_file.split("_")[2]
                bot.send_message(chat_id = chat_id, text = "I'm so sorry 🥺, I couldn't detect anything..\nRetry sending another image, I'll do the best! 😉")
            
            # The prediction was successful: resend the predicted image back to the user
            else:
                chat_id = new_file.split("_")[1]
                bot.send_photo(chat_id = chat_id, photo = open(f"/home/davidecas/iRecognize/logstash/csv/detected_photos/{new_file}", "rb"))
            
        before = after

# Append a single row into the "filenames.csv" file
def csv_upload(user_name, file_name):
    # add row to CSV file
    with open("/home/davidecas/iRecognize/logstash/csv/filenames.csv", "a") as f:
        f.write(user_name + "_" + file_name + "\n")

# Erase the folder with photos and csv with filenames
def erase():
    while 1:
        if input() == "erase":
            path1 = "/home/davidecas/iRecognize/logstash/csv/photos/"
            path2 = "/home/davidecas/iRecognize/logstash/csv/detected_photos/"

            for root, dirs, files in os.walk(path1):
                for file in files: os.remove(os.path.join(root, file))

            for root, dirs, files in os.walk(path2):
                for file in files: os.remove(os.path.join(root, file))

            os.remove("/home/davidecas/iRecognize/logstash/csv/filenames.csv")
            open('/home/davidecas/iRecognize/logstash/csv/filenames.csv', 'wb')

print("Bot started..")
threading.Thread(target=directory_checker).start()
threading.Thread(target=erase).start()
bot.polling(True)
