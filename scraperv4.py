import bs4
import requests
import re
import textract
from mrjob.job import MRJob


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


def check_request(page_url):
    try:
        request = requests.get(page_url)
    except requests.exceptions.RequestException:
        request = None
    return request

def get_pdf_url(image_number):
    PDF_url = None
    url_base = "http://docquery.fec.gov/cgi-bin/fecimg/?"
    page_url = url_base + image_number
    request = check_request(page_url)
    if request:
        html = request.text.encode('iso-8859-1')
        soup = bs4.BeautifulSoup(html, "lxml")
        part_noscript = soup.find("noscript")
        if part_noscript:
            part_src = part_noscript.find("embed")
            if part_src:
                PDF_url = part_src.get("src")
        return PDF_url
    elif not request:
        return None
    else:
        return None

def pdf_check(request):
    soup = bs4.BeautifulSoup(request.text, "lxml")
    h1_section = soup.h1
    if h1_section:
        check = re.search("Page Not Found", h1_section.text)
        if check:
            return False
        elif not check:
            return True
    elif not h1_section:
        return True  


def get_address(file_name, indiv_ID, indiv_Name, indiv_Zip):
    text = textract.process(file_name)
    string = bytes.decode(text)
    PDF_mailings = re.findall('\Mailing Address (.*?)\n', string)  
    num_on_page = get_address_num(string, indiv_ID, indiv_Name, indiv_Zip)
    if not num_on_page:
        num_on_page = 0
    if len(PDF_mailings) >= num_on_page:
        if type(num_on_page) != list and num_on_page:
            return PDF_mailings[num_on_page - 1]
        elif type(num_on_page) == list or not num_on_page:
            num_on_page = lower_case_fix(string, indiv_ID, num_on_page)
            if type(num_on_page) != list and num_on_page:
                return PDF_mailings[num_on_page - 1]
    if len(PDF_mailings) >= num_on_page or num_on_page:
        if type(num_on_page) == list or not num_on_page:
            return "Read_Error"
    else:
        return "Read_Error"
    return "Read_Error"


def lower_case_fix(string, indiv_ID, num_on_page):
    PDF_IDs = re.findall('Transaction ID : (.*?)\n', string)
    length_IDs = len(PDF_IDs)
    for num in range(0,length_IDs):
        if PDF_IDs and len(PDF_IDs) >= (num + 1) and PDF_IDs[num].lower() == indiv_ID.lower():
            num_on_page = num + 1
            break
    return num_on_page


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
    PDF_Names = re.findall('Initial\)\n\n(.*?)\n\nDate', string)
    PDF_Zips = re.findall('Zip Code\n(.*?)\n', string)
    num_on_page = None

    for num in range(0,3):
        if PDF_IDs and len(PDF_IDs) >= (num + 1) and PDF_IDs[num] == indiv_ID:
            num_on_page = num + 1
            break
        elif len(PDF_IDs) < (num + 1) or PDF_IDs[num] != indiv_ID:
            if PDF_Names and len(PDF_Names) >= (num + 1) and PDF_Names[num][3:] == indiv_Name:
                num_on_page = record_page_num(num_on_page, num)
            elif len(PDF_Names) < (num + 1) or PDF_Names[num][3:] != indiv_Name:
                if PDF_Zips and len(PDF_Zips) >= (num + 1) and PDF_Zips[num] == indiv_Zip:
                    num_on_page = record_page_num(num_on_page, num)
    return num_on_page

def process(image_number, indiv_ID, indiv_Name, indiv_Zip):
    url = get_pdf_url(image_number)
    if url:
        end = re.findall('pdf/(.*?).pdf', url)
        end_list = re.split("/", end[0])
        response = requests.get(url)
        if not response:
            return "request_failure"
        check_pdf = pdf_check(response)
        if check_pdf:
            file_name = '/tmp/' + end_list[2]+  '.pdf'
            with open(file_name, 'wb') as f:
                f.write(response.content)
            address = get_address(file_name, indiv_ID, indiv_Name, indiv_Zip)
            f.close()
            return address
        if not check_pdf:
            return "PDF not found"
    if not url:
        return "request/soup_failure"

def get_name(name):
    name_split = name.split(", ")
    if len(name_split) > 1:
        indiv_Name = name_split[1] + " " + name_split[0]
    elif len(name_split) <= 1:
        indiv_Name = name_split[0]
    return indiv_Name


class MRJob_data(MRJob):

    def get_chicago_zip_codes(self):
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
  

    def mapper_init(self):
        self.chicago_zip_codes = self.get_chicago_zip_codes()

    def mapper(self, _, line):
        """
        yields:
          key: a string, sub_ID from FEC data
          value: a string, street address 
        """
        line_spilt = line.split("|")
        if len(line_spilt) >= 10:
            indiv_Zip = line_spilt[10]
            if indiv_Zip[0:5] in self.chicago_zip_codes:
                if len(indiv_Zip) > 5:
                    indiv_Zip = indiv_Zip[0:5] + '-' + indiv_Zip[5:]
                yield line, indiv_Zip

    def combiner(self, line, zip_code):
        """
        yields:
          key: a string, sub_ID from FEC data
          value: a string, street address 
        """

        indiv_Zip = "".join(zip_code)

        yield line, indiv_Zip

    def reducer_init(self):
        self.count_too_short = 0

    def reducer(self, line, zip_code):
        """
        yields:
          key: a string, sub_ID from FEC data
          value: a string, street address 
        """
        line_spilt = line.split("|")
        length_line = len(line_spilt)
        if length_line >= 16:
            indiv_Zip = "".join(zip_code)
            indiv_ID = line_spilt[16]
            image_number = line_spilt[4]
            if length_line >= 20:
                sub_ID = line_spilt[20]
            if length_line < 20:
                sub_ID = "length_too_short_" + str(self.count_too_short)
                self.count_too_short += 1
            indiv_Name = get_name(line_spilt[7])
            address = process(image_number, indiv_ID, indiv_Name, indiv_Zip)

            yield sub_ID, address

if __name__ == '__main__':
    MRJob_data.run()