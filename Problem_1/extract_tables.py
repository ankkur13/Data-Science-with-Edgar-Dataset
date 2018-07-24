from bs4 import BeautifulSoup
import urllib.request
import csv
import sys
import os
import zipfile
import logging
from logging.handlers import TimedRotatingFileHandler
import time
import datetime
import re
import shutil

import boto
import boto.s3
import sys
from boto.s3.key import Key

# Function to upload to AWS S3
def uploadToS3(destinationPath, filePath, arg_AWSuser, arg_AWSpass):
    
    AWS_ACCESS_KEY_ID = arg_AWSuser
    AWS_SECRET_ACCESS_KEY = arg_AWSpass

    bucket_name = AWS_ACCESS_KEY_ID.lower()
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)


    bucket = conn.create_bucket(bucket_name,location=boto.s3.connection.Location.DEFAULT)

    testfile = filePath
    print ('Uploading '+testfile+' to Amazon S3 bucket '+bucket_name)
    def percent_cb(complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()

    print('here1')
    k = Key(bucket)
    k.key = destinationPath+"/"+testfile
    k.set_contents_from_filename(testfile,cb=percent_cb, num_cb=10)

# Function to generate the first URL
def generate_link(cik, accession):
    logger.info('function: Generating EDGAR FTP File Path for Filing Details')
    cik = str(cik)
    accession = str(accession)
    cik = cik.lstrip('0')
    acc = re.sub(r'[-]', r'', accession)
    url = 'https://www.sec.gov/Archives/edgar/data/' + cik + '/' + acc + '/' + accession + '/-index.htm'
    try:
        logger.info('Generated Filing Details URL -> {}'.format(url))
        page_open = urllib.request.urlopen(url)
        generate_10q_link(url)
        
        page_open.close()
        
    except:
        logger.critical('Invalid URL entered -> {}'.format(url))
        print("Invalid URL -> {}".format(url))


# Function to generate the second URL - 10q
def generate_10q_link(url):
    logger.info('function: Generating EDGAR FTP File Path for Form 10-Q')
    final_url = ""
    html = urllib.request.urlopen(url)
    soup = BeautifulSoup(html, "html.parser")
    
    html.close()
    
    all_page_links = soup.findAll('a')
    
    for i in range(len(all_page_links)):
        if '10q' in all_page_links[i].text:
            final_url = all_page_links[i].get('href')
    
    final_link = "https://www.sec.gov" + final_url
    logger.info('Generated 10-Q URL -> {}'.format(url))
    get_next_page(final_link)
    
    
# Function to fetch second page - 10Q
def get_next_page(url):
    try:
        logger.info('function: Fetching page_html from Form 10-Q and parsing')
        p_html = urllib.request.urlopen(url)
        g_soup = BeautifulSoup(p_html, "html.parser")
        
        p_html.close()
        
        get_all_tables(g_soup)
    except:
        logger.critical('Unable to fetch, invalid URL entered -> {}'.format(url))
        print("Invalid URL -> {}".format(url))

    
# Function to get all tables from a parsed link soup
def get_all_tables(g_soup):
    logger.info('function: Fetching all tables from parsed Form 10-Q html file')
    a_tables = g_soup.find_all('table')
    all_datatables(g_soup, a_tables)
    return 0


# Function to create a folder name containing CSVs'
def get_folder_name(g_soup):
    title = g_soup.find('filename').contents[0]
    if ".htm" in title:
        folder_name = title.split(".htm")
        return folder_name[0]


# Function to create zip the folder
def zip_dir(path_dir, path_file_zip=''):    
    '''Describes how the function works.
    
    arg path_dir: the path to the folder to be zipped
    arg path_file_zip: the path (a string) to the file (zipped folder that has to be created)
    
    1) If arg path_file_zip is empty, create a file.
        os.path.join(dirpath, name) creates a path
            arg dirpath: the path to the directory 
            arg name: the name of
                os.path.dirname(path_dir) gives the path of the directory the folder is in.
                os.path.basename(path_dir) gives the name of the folder itself.
        
    2) Use the below zipfile Library function to zip a folder
        class zipfile.ZipFile(file, mode='r', compression=ZIP_STORED, allowZip64=True)
            zipfile.ZIP_DEFLATED: numeric constant for the usual ZIP compression method
        
        Method walk() generates file names in directory tree by walking either top-down or bottom-up.
        
        ZipFile.write(filename, arcname=None, compress_type=None)
            arg filename: name of the file written
            arg arcname: archive name
            arg compress_type overrides value given for compression parameter to constructor for new entry
            
            os.path.relpath(arg1, arg2) will give the relative path of arg1 from inside the directory of arg2
            os.path.pardir will give the parent directory
    
    '''
    if not path_file_zip:
        path_file_zip = os.path.join(os.path.dirname(path_dir), os.path.basename(path_dir) + '.zip')
    
    logger.info('function: Zipping Files')
    with zipfile.ZipFile(path_file_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        
        for root, dirs, files in os.walk(path_dir):
            for file_or_dir in files + dirs:
                zip_file.write(
                    # filename
                    os.path.join(root, file_or_dir),
                        # arcname
                        os.path.relpath(os.path.join(root, file_or_dir),os.path.join(path_dir, os.path.pardir)))


# Function to assure the path exists using makedirs(): a recursive directory creation function
def assure_path_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)

        
# Function to check the header tag
def checkheadertag(param):
    flag = "false"
    datatabletags = ["center","bold"]
    for x in datatabletags:
        if x in param:
            flag="true"
    return flag


# Function to get the table data
def get_table_data(table):
    table_data = []
    all_tr = table.find_all('tr')
    for tr in all_tr:
        data = []
        td_list = []
        all_td = tr.find_all('td')
        for td in all_td:
            re_td = td.text;
            re_td = re.sub(r"['()]","",str(re_td))
            re_td = re.sub(r"[$]","",str(re_td))
            if(len(re_td)>1):
                re_td = re.sub(r"[—]","",str(re_td))
                td_list.append(re_td)
        data = ([z.encode('utf-8') for z in td_list])
        table_data.append([z.decode('utf-8').strip() for z in data])
    return table_data


# Function to check the style of td and tr tags
def checktag(param):
    flag = "false"
    datatabletags = ["background", "bgcolor", "background-color"]
    for x in datatabletags:
        if x in param:
            flag = "true"
    return flag

# Function to get the final data tables
def all_datatables(g_soup, a_tables):
    logger.info('function: Fetching all DATA TABLES from all tables found')
    count = 0
    allheaders=[]
    logger.info('for loop: Looping accross tables to find background color pattern')
    for table in a_tables:
        bluetables = []
        trs = table.find_all('tr')
        for tr in trs:
            if checktag(str(tr.get('style'))) == "true" or checktag(str(tr)) == "true":
                bluetables = get_table_data(tr.find_parent('table'))
                break
            else:
                tds = tr.find_all('td')
                for td in tds:
                    if checktag(str(td.get('style'))) == "true" or checktag(str(td)) == "true":
                        bluetables = get_table_data(td.find_parent('table'))
                        break
            if not len(bluetables) == 0:
                break
        if not len(bluetables) == 0:
            count += 1
            ptag = table.find_previous('p');
            while ptag is None and checkheadertag(ptag.get('style')) == "false" and len(ptag.text)<=1:
                ptag = ptag.find_previous('p')
                if checkheadertag(ptag.get('style')) == "true" and len(ptag.text)>=2:
                    global name
                    name = re.sub(r"[^A-Za-z0-9]+", "", ptag.text)
                    if name in allheaders:
                        hrcount += 1
                        hrname = name+"_"+str(hrcount)
                        allheaders.append(hrname)
                    else:
                        hrname = name
                        allheaders.append(hrname)
                        break
            
            global folder_name
            folder_name = get_folder_name(g_soup)
            
            # os.getcwd() returns a string representing the current working directory
            global path_folder
            path_folder = str(os.getcwd()) + "/" + folder_name
            assure_path_exists(path_folder)
            if(len(allheaders)==0):
                filename = folder_name+"-"+str(count)
            else:
                filename = allheaders.pop()
            csvname = filename + ".csv"
            csvpath = path_folder + "/" + csvname
            with open(csvpath, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(bluetables)
            zip_dir(path_folder)

def close_program(arg_AWSuser, arg_AWSpass):
    try:
        shutil.rmtree(path_folder, ignore_errors=False, onerror=None)
        print(path_folder)
        print(folder_name)
        logger.info('function: Updating Extracted_Tables to AWS S3')
        uploadToS3("Extracted_Tables", folder_name+'.zip', arg_AWSuser, arg_AWSpass)
        time.sleep(5)
        print('Good Going')
        logger.info('function: Updating Activity_Log to AWS S3')
        uploadToS3("Activity_Log", logfilename, arg_AWSuser, arg_AWSpass)
    except:
        logger.warning('Error ending the Program')    
    logger.info('PROGRAM ENDS')


def main_function():
    
    cik = sys.argv[1]
    accession = sys.argv[2]
    
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y_%m_%d_%H-%M-%S')
    
    global logfilename
    logfilename = 'log_Edgar_'+ str(cik) + '_' + st + '.log'
    
    global logger
    logger = logging.getLogger(str(cik))
    
    print(len(logger.handlers))
    logger.setLevel(logging.DEBUG)
    
    fh = TimedRotatingFileHandler(logfilename,  when='midnight')
    #fh.suffix = '%Y_%m_%d.log'

    logger.addHandler(fh)
    
    formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(levelname)-8s %(message)s', datefmt = '%a, %d %b %Y %H:%M:%S')
    fh.setFormatter(formatter)
        
    logger.info('PROGRAM STARTED')
    
    generate_link(cik, accession)
       
    close_program(sys.argv[3], sys.argv[4])


if __name__ == '__main__':
    main_function()

