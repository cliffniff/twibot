import mysql.connector as db_handler
import pytable
from bs4 import BeautifulSoup
import datetime
import time
import re
import multiprocessing as mp
import requests.exceptions
from urllib.parse import urljoin
import colorama

colorama.init()

log_file = open('log.txt', 'a')


def log(data, phid, info=True):
    if info:
        print(colorama.Fore.YELLOW + colorama.Back.RESET + '[INFO]' + ' [{0}]'.format(
            phid) + colorama.Fore.GREEN + ' ' + data + '\n')
        log_file.write('[INFO]' + ' [{0}] '.format(phid) + data + '\n')
    else:
        print(colorama.Back.RED + colorama.Fore.LIGHTYELLOW_EX + '\033[1m' + '[ERROR]' + '\033[0m' + colorama.Fore.LIGHTYELLOW_EX + ' [{0}]'.format(
            phid) + colorama.Fore.YELLOW + ' ' + data + '\n')
        log_file.write('[ERROR]' + ' [{0}] '.format(phid) + data + '\n')


def crawl(p_id, file_links, host, user, port, dbname, passwd, dbtable):
    connector = ','
    db = db_handler.connect(host=host, user=user, database=dbname, port=port, passwd=passwd)
    cur = db.cursor(buffered=True)
    while not file_links.empty():
        r_url = file_links.get()
        url = r_url.strip()
        try:
            table = pytable.Table(12, 2)
            table.make()
            table.put(1, 1, 'URL')
            table.put(2, 1, 'Status-Code')
            table.put(3, 1, 'Process ID')
            table.put(5, 1, 'Images')
            table.put(6, 1, 'Links')
            table.put(7, 1, 'Only Text')
            table.put(8, 1, 'Description')
            table.put(9, 1, 'Title')
            table.put(10, 1, 'Icon')
            table.put(11, 1, 'HTML')
            start_time = time.time()
            log('No. of crawled : '+str(1000000 - file_links.qsize()), p_id)
            table.put(1, 2, url)
            table.put(3, 2, str(p_id))
            print('-' * 40)
            print('Requesting...')
            req = requests.get(url, headers={'User-Agent': 'TwiBot 4.0 (Windows; U; Windows NT 5.1; de; rv:1.9.2.3)', 'Accept-Language': 'en-US'}, timeout=20)
            print("Done...")
            if req.ok:
                table.put(2, 2, str(req.status_code))
                soup = BeautifulSoup(req.text, 'lxml')
                images = soup.find_all('img', src=True)
                if len(images) == 0 or images is None:
                    images = 'No Images'
                else:
                    for image in images:
                        if image['src'].strip()[:5] == 'data:':
                            if 'alt' in image:
                                alt_text = image['alt']
                                new_alt_text = ''
                                for character in alt_text:
                                    if character == '\n' or character == '\r':
                                        continue
                                    else:
                                        new_alt_text += character
                                images[images.index(image)] = image['src'] + '::' + new_alt_text
                            else:
                                images[images.index(image)] = image['src']
                        else:
                            img_url = urljoin(url, image['src'].strip())
                            if 'alt' in image:
                                images[images.index(image)] = img_url + '::' + image['alt']
                            else:
                                images[images.index(image)] = img_url
                table.put(5, 2, connector.join(images))
                links = soup.find_all('a', href=True, limit=5)
                if len(links) == 0 or links is None:
                    links = 'No links'
                else:
                    for link in links:
                        if link['href'][:10] != 'javascript:':
                            a_url = urljoin(url, link['href'].strip())
                            if link.text != '' or link.text is not None:
                                link_text = link.text
                                new_link_text = ''
                                for char in link_text:
                                    if char == '\n' or char == '\r':
                                        continue
                                    else:
                                        new_link_text += char
                                links[links.index(link)] = a_url + '::' + new_link_text
                            else:
                                links[links.index(link)] = a_url
                table.put(6, 2, connector.join(links))
                texts = soup.text
                texts = re.sub(r'<script.*?>.*?<\/script>|<style.*?>.*?<\/style>', '', texts, flags=re.IGNORECASE)
                if texts is None:
                    texts = 'No texts'
                table.put(7, 2, len(texts))
                if soup.find('meta', attrs={'name': 'description'}) is not None:
                    description = soup.find('meta', attrs={'name': 'description'})['content'].strip()
                elif soup.p is not None and soup.p.string is not None:
                    description = soup.p.string.strip()
                else:
                    description = 'No Description Available'
                table.put(8, 2, description)
                if soup.find('meta', attrs={'name': 'title'}) is not None:
                    title = soup.find('meta', attrs={'name': 'title'})['content'].strip()
                elif soup.title is not None and soup.title.string is not None:
                    title = soup.title.string.strip()
                else:
                    title = url
                table.put(9, 2, title)
                if soup.find('link', attrs={'rel': 'shortcut icon'}) is not None:
                    icon = soup.find('link', attrs={'rel': 'shortcut icon'})['href'].strip()
                elif soup.find('link', attrs={'rel': 'icon'}) is not None:
                    icon = soup.find('link', attrs={'rel': 'icon'})['href'].strip()
                else:
                    icon = 'https://cdn.twixall.com/search/images/no_icon.png'
                icon = urljoin(url, icon.strip())
                table.put(10, 2, icon)
                html = req.text
                table.put(11, 2, len(html))
                print(table)
                del table
                print('-' * 40)
                log('Preparing DB Query', p_id)
                sql = "INSERT INTO `" + dbtable + "`(url, title, description, text_only, images, links, html, icon) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)"
                log('Prepared.', p_id)
                log('Executing Query.', p_id)
                cur.execute(sql, (
                    url, str(title).strip(), str(description).strip(), str(texts).strip(), str(images).strip(), str(links).strip(), str(html).strip(),
                    str(icon.strip()).strip()))
                log('Executed.', p_id)
                log('Commiting.', p_id)
                db.commit()
                log('Commited.', p_id)
                log('Spent {0} seconds.'.format(time.time() - start_time), p_id)
                log('Finished.', p_id)
                log('Crawling on {0} started at {1}'.format(url, str(datetime.datetime.now())), p_id)
                file_links.task_done()
            else:
                log('Bad Code : '.format(req.status_code), p_id, False)
                file_links.task_done()
            time.sleep(0.01)
        except ConnectionRefusedError:
            print(colorama.Fore.RED + "Database server is offline..." + colorama.Fore.RESET)
        except requests.exceptions.ConnectionError:
            log('An connection error', p_id, False)
        except requests.exceptions.TooManyRedirects:
            log('Too many redirects.', p_id, False)
        except requests.exceptions.Timeout:
            log('Requests timed out.', p_id, False)
        except Exception as e:
            log("Unknown error occured! - " + str(e), p_id, False)
            file_links.task_done()
    log_file.close()


if __name__ == '__main__':
    mp.freeze_support()
    import socket
    import requests
    import os
    p_credits = "------------------------------------------\n|   Twixall © - 2019                     |\n\r|   TwiBot 4.0                           |\n\r|   This tool is only for use of Twixall |\n\r------------------------------------------\n\r"
    print(colorama.Style.BRIGHT + colorama.Fore.GREEN + p_credits + colorama.Style.RESET_ALL)
    print(colorama.Fore.RED + "**This Program requires internet connection.**" + colorama.Fore.RESET + '\n')
    processes = []
    queue = mp.JoinableQueue()
    last_details = 'last_details.txl'
    try:
        if os.path.exists(last_details):
            prf = input("Do you prefer to load last time values for host, port and table (Y)es or (N)o : ")
            if prf == 'Y':
                lddata = open(last_details).read()
                if lddata != '' and lddata is not None:
                    lddata = lddata.split('\n')
                    for i in lddata:
                        f = i.split(':')[0].strip()
                        l = i.split(':')[1].strip()
                        if f == 'host':
                            dbhost = l
                        elif f == 'port':
                            if l.isdigit():
                                dbport = int(l)
                            else:
                                dbport = int(input("MySQL Server port : "))
                        elif f == 'table':
                            db_table = l
                        elif f == 'name':
                            db_name = l
                    db_user = input("MySQL Server user : ")
                    db_pass = input("MySQL Server password : ")
                else:
                    dbhost = input("\nMysql Server host : ")
                    dbport = int(input("MySQL Server port : "))
                    db_name = input("MySQL Server DB : ")
                    db_user = input("MySQL Server user : ")
                    db_pass = input("MySQL Server password : ")
                    db_table = input("MySQL DB table : ")
                print(colorama.Fore.GREEN + "Host : {0}\nPort : {1}\nDatabase : {2}\nTable : {3}".format(dbhost, dbport,db_name, db_table) + colorama.Fore.RESET)
            elif prf == 'N':
                dbhost = input("\nMysql Server host : ")
                dbport = int(input("MySQL Server port : "))
                db_name = input("MySQL Server DB : ")
                db_user = input("MySQL Server user : ")
                db_pass = input("MySQL Server password : ")
                db_table = input("MySQL DB table : ")
            else:
                print(colorama.Fore.GREEN + "Invalid Input...Defaulting to Load" + colorama.Fore.RESET)
                lddata = open(last_details).readlines()
                if lddata != '' or lddata is not None or lddata != []:
                    for i in lddata:
                        f = i.split(':')[0].strip()
                        l = i.split(':')[1].strip()
                        if f == 'host':
                            dbhost = l
                        elif f == 'port':
                            if l.isdigit():
                                dbport = int(l)
                            else:
                                dbport = int(input("MySQL Server port : "))
                        elif f == 'table':
                            db_table = l
                        elif f == 'name':
                            db_name = l
                else:
                    dbhost = input("\nMysql Server host : ")
                    dbport = int(input("MySQL Server port : "))
                    db_name = input("MySQL Server DB : ")
                    db_user = input("MySQL Server user : ")
                    db_pass = input("MySQL Server password : ")
                    db_table = input("MySQL DB table : ")
                print(colorama.Fore.GREEN + "Host : {0}\nPort : {1}\nDatabase : {2}\nTable : {3}".format(dbhost, dbport,db_name,db_table) + colorama.Fore.RESET)
                db_user = input("MySQL Server user : ")
                db_pass = input("MySQL Server password : ")
        else:
            dbhost = input("\nMysql Server host : ")
            dbport = int(input("MySQL Server port : "))
            db_name = input("MySQL Server DB : ")
            db_user = input("MySQL Server user : ")
            db_pass = input("MySQL Server password : ")
            db_table = input("MySQL DB table : ")
    except IndexError:
        print("Invalid file")
        dbhost = input("\nMysql Server host : ")
        dbport = int(input("MySQL Server port : "))
        db_name = input("MySQL Server DB : ")
        db_user = input("MySQL Server user : ")
        db_pass = input("MySQL Server password : ")
        db_table = input("MySQL DB table : ")
        print(colorama.Fore.GREEN + "Host : {0}\nPort : {1}\nDatabase : {2}\nTable : {3}".format(dbhost, dbport, db_name, db_table) + colorama.Fore.RESET)
    with open(last_details, 'w') as f:
        lddata = "host:{0}\nport:{1}\ntable:{2}\nname:{3}".format(dbhost, dbport, db_table, db_name)
        f.write(lddata)
        f.close()
    print(colorama.Fore.GREEN + "Checking server status..." + colorama.Fore.RESET)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((dbhost, int(dbport)))
        s.shutdown(2)
        print(colorama.Fore.GREEN + "Server is online..." + colorama.Fore.RESET)
    except ConnectionRefusedError:
        print(colorama.Fore.RED + "Server is offline..." + colorama.Fore.RESET)
        exit()
    print("No. of processes instantiating " + str(8))
    lines = open('index.txt', 'r').readlines()
    for line in lines:
        queue.put(line)
    for _ in range(8):
        p = mp.Process(target=crawl, args=(_, queue, dbhost, db_user, dbport, db_name, db_pass, db_table))
        p.start()
        processes.append(p)
    queue.join()
    for process in processes:
        process.join()
        processes.remove(process)