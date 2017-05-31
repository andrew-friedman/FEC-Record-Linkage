import bs4
import requests
import re
import textract
# from pytesseract import *
from mrjob.job import MRJob
# from mrjob.protocol import (JSONValueProtocol, RawValueProtocol)

file_cand_data = open('reducer_summary_files/web_cand.txt', 'rb')

def get_chicago_zip_codes():
    zip_codes = set(['60601', '60602', '60603', '60604', '60605',
        '60606', '60607', '60608', '60609', '60610', '60611',
        '60612', '60613', '60614', '60615', '60616', '60617',
        '60618', '60619', '60620', '60621', '60622', '60623',
        '60624', '60625', '60626', '60628', '60629', '60630',
        '60631', '60632', '60633', '60634', '60636', '60637',
        '60638', '60639', '60640', '60641', '60642', '60643',
        '60644', '60645', '60646', '60647', '60649', '60651',
        '60652', '60653', '60654', '60655', '60656', '60657',
        '60659', '60660', '60661', '60664', '60666', '60668',
        '60669', '60670', '60673', '60674', '60675', '60677',
        '60678', '60680', '60681', '60682', '60684', '60685',
        '60686', '60687', '60688', '60689', '60690', '60691',
        '60693', '60694', '60695', '60696', '60697', '60699',
        '60701'])
    return zip_codes


def get_pdf_url(image_number):
    pdf_url = None
    url_base = "http://docquery.fec.gov/cgi-bin/fecimg/?"
    page_url = url_base + image_number
    request = requests.get(page_url)
    html = request.text.encode('iso-8859-1')
    soup = bs4.BeautifulSoup(html, "lxml")
    part_noscript = soup.find("noscript")
    if part_noscript:
        part_src = part_noscript.find("embed")
        if part_src:
            pdf_url = part_src.get("src")

    return pdf_url

def get_address(file_name, indiv_ID, indiv_Name, indiv_Zip):
    text = textract.process(file_name)
    string = bytes.decode(text)
    PDF_mailings = re.findall('\Mailing Address (.*?)\n', string)  
    print(PDF_mailings)
    num_on_page = get_address_num(string, indiv_ID, indiv_Name, indiv_Zip)
    if len(PDF_mailings) != 3:
        print("hello", len(PDF_mailings), num_on_page)

    if len(PDF_mailings) >= num_on_page:
        if type(num_on_page) != list and num_on_page:
            return PDF_mailings[num_on_page - 1]
        if type(num_on_page) == list or not num_on_page:
            num_on_page = lower_case_fix(string, indiv_ID, num_on_page)
            if type(num_on_page) != list and num_on_page:
                return PDF_mailings[num_on_page - 1]
        elif type(num_on_page) == list:
            return "Multiple_Error"
        elif not num_on_page:
            return "Read_Error"



def lower_case_fix(string, indiv_ID, num_on_page):
    PDF_IDs = re.findall('Transaction ID : (.*?)\n', string)
    for num in range(0,3):
        l_PDF_ID = PDF_IDs[num].lower()
        l_indiv_ID = indiv_ID.lower()
        if len(PDF_IDs) >= (num + 1) and l_PDF_ID == l_indiv_ID:
            num_on_page = num + 1
            break
    return num_on_page

    


def get_string(image_number):
    url = get_pdf_url(image_number)
    if url:
        end = re.findall('pdf/(.*?).pdf', url)
        end_list = re.split("/", end[0])
        response = requests.get(url)
        file_name = '/tmp/' + end_list[2]+  '.pdf'
        with open(file_name, 'wb') as f:
            f.write(response.content)
        text = textract.process(file_name)
        f.close()
        string = bytes.decode(text)
        return string
    elif not url:
        return None

def record_page_num(num_on_page, num):
    if not num_on_page:
        num_on_page = num + 1
    elif type(num_on_page) == list:
        num_on_page.append(num + 1)
    elif num_on_page:
        num_on_page = [num_on_page, num + 1]
    return num_on_page


def get_address_num(string, indiv_ID, indiv_Name, indiv_Zip):
    PDF_IDs = re.findall('Transaction ID : (.*?)\n', string)
    print(PDF_IDs)
    PDF_Names = re.findall('Initial\)\n\n(.*?)\n\nDate', string)
    PDF_Zips = re.findall('Zip Code\n(.*?)\n', string)
    num_on_page = None

    for num in range(0,3):
        if len(PDF_IDs) >= (num + 1) and PDF_IDs[num] == indiv_ID:
            num_on_page = num + 1
            break
        elif len(PDF_IDs) < (num + 1) or PDF_IDs[num] != indiv_ID:
            if len(PDF_Names) >= (num + 1) and PDF_Names[num][3:] == indiv_Name:
                num_on_page = record_page_num(num_on_page, num)
            elif len(PDF_Names) < (num + 1) or PDF_Names[num][3:] != indiv_Name:
                print(PDF_Zips)
                print()
                if len(PDF_Zips) >= (num + 1) and PDF_Zips[num] == indiv_Zip:
                    num_on_page = record_page_num(num_on_page, num)
    return num_on_page

def process(image_number, indiv_ID, indiv_Name, indiv_Zip):
    url = get_pdf_url(image_number)
    if url:
        end = re.findall('pdf/(.*?).pdf', url)
        end_list = re.split("/", end[0])
        response = requests.get(url)
        file_name = '/tmp/' + end_list[2]+  '.pdf'
        with open(file_name, 'wb') as f:
            f.write(response.content)
        address = get_address(file_name, indiv_ID, indiv_Name, indiv_Zip)
        f.close()
        return address
    elif not url:
        print("yeas")
        return None

