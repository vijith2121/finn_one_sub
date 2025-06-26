import scrapy
from finn_one_sub.items import Product
from lxml import html
import os
from email import message_from_string
from email.policy import default
from datetime import date


class Finn_one_subSpider(scrapy.Spider):
    name = "finn_one_sub"

    # def start_requests(self):
    #     folder_path = os.path.dirname(os.path.abspath(__file__))
    #     for file_name in os.listdir(folder_path):
    #         if file_name.endswith(".mhtml"):
    #         # if file_name.endswith(".mhtml"):
    #             # print(file_name)
    #             file_path = f"file://{os.path.abspath(os.path.join(folder_path, file_name))}"

    #             # file_path = 'file:///home/vijith/Downloads/OneDrive_2025-04-03/ADCB%20XUNB%20CM%20AND%20PRODUCT%20DETAILS/10007324/10007324%202.mhtml'
    #             yield scrapy.Request(
    #                 url=file_path,
    #                 callback=self.parse,
    #             )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.finn_uae_list = []       # For main files
        self.finn_uae_sub_list = []   # For sub files
        self.total_requests = 0       # Track total files to process
        self.processed_responses = 0  # Track responses received
    
    def start_requests(self):
        folder_path = os.path.dirname(os.path.abspath(__file__))
        for file_name in os.listdir(folder_path):
            if file_name.endswith(".mhtml"):
                # print(file_name)
                self.total_requests += 1
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                yield scrapy.Request(
                    url='file://' + file_path,
                    callback=self.parse,
                    meta={
                        'file_name': file_name,
                        'raw_mhtml': content
                    }
                )

    def parse(self, response):
        # and '_account' in file_name
        file_name = response.meta.get('file_name')
        finn_uae_list, finn_uae_sub_list = [], []
        if file_name:
            
            if '_Account' in file_name or '_account' in file_name:
                finn_uae = self.parse1(response, response.meta)
                if finn_uae:
                # yield Product(**{'res': finn_uae})
                    print('finn_uae===========================')
                    print(finn_uae)
                    print('=============================')
                    self.finn_uae_list.append(finn_uae)
            elif '_Overdue' in file_name or '_overdue' in file_name:
                finn_uae_sub = self.parse2(response, response.meta)
                if finn_uae_sub:
                    print('finn_uae_sub===========================')
                    print(finn_uae_sub)
                    print('=============================')
                    self.finn_uae_sub_list.append(finn_uae_sub)
            # else:
            #     finn_uae_sub = self.parse2(response, response.meta)
            #     if finn_uae_sub:
            #         self.finn_uae_sub_list.append(finn_uae_sub)

        self.processed_responses += 1
        if self.processed_responses == self.total_requests:

            if self.finn_uae_list and self.finn_uae_sub_list:
                for item in self.generate_items():
                    yield Product(**item)
            else:
                print("No data found in one or both lists")


    def parse2(self, response, meta):
        mhtml_raw = meta['raw_mhtml']

        # Step 1: Parse as email
        msg = message_from_string(mhtml_raw, policy=default)

        # Step 2: Extract the HTML part
        html_body = None
        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type == 'text/html':
                    html_body = part.get_content()
                    break
        else:
            if msg.get_content_type() == 'text/html':
                html_body = msg.get_content()

        if not html_body:
            self.logger.warning("No HTML found in: %s", response.url)
            return

        # Step 3: Parse HTML with lxml
        parser = html.fromstring(html_body)

        Overdue_Since = ''.join(
            parser.xpath("//div[contains(text(), 'Overdue Since')]//parent::div/div[2]//text()")
        ).strip()
        No_of_Installments_Overdue = ''.join(
            parser.xpath("//div[contains(text(), 'No. of Installments Overdue')]//parent::div/div[2]//text()")
        ).strip()
        Installment_Start_Date = ''.join(
            parser.xpath("//div[contains(text(), 'Installment Start Date')]//parent::div/div[2]//text()")
        ).strip()
        Installment_Overdue = ''.join(
            parser.xpath("//div[contains(text(), 'Installment Overdue')]//parent::div/div[2]//text()")
        ).strip()
        Principal_Overdue = ''.join(
            parser.xpath("//div[contains(text(), 'Principal Overdue')]//parent::div/div[2]//text()")
        ).strip()
        Interest_Overdue = ''.join(
            parser.xpath("//div[contains(text(), 'Interest Overdue')]//parent::div/div[2]//text()")
        ).strip()
        Charges_Overdue = ''.join(
            parser.xpath("//div[contains(text(), 'Charges Overdue')]//parent::div/div[2]//text()")
        ).strip()
        Last_Payment_Date = ''.join(
            parser.xpath("//div[contains(text(), 'Last Payment Date')]//parent::div/div[2]//text()")
        ).strip()
        Last_Payment_Amount = ''.join(
            parser.xpath("//div[contains(text(), 'Last Payment Amount')]//parent::div/div[2]//text()")
        ).strip()
        Bucket = ''.join(
            parser.xpath("//div[contains(text(), 'Bucket')]//parent::div/div[2]//text()")
        ).strip()
        Account_no = ''.join(
            parser.xpath("//div[@id='selectedLoanNo_chosen']//span//text()")
        )
        cif_cid = ''.join(
            parser.xpath("//label[contains(text(), 'CIF#')]//parent::div/text()")
        )
        cif_cid = ''.join(
            [i.strip() for i in cif_cid if i.strip()]
        ) if cif_cid else ''
        scrape_date = date.today()
        data = {
            'Overdue_Since': Overdue_Since,
            'No_of_Installments_Overdue': No_of_Installments_Overdue,
            'Installment_Start_Date': Installment_Start_Date,
            'Installment_Overdue': Installment_Overdue,
            'Principal_Overdue': Principal_Overdue.split()[0].strip() if Principal_Overdue else '',
            'Interest_Overdue': Interest_Overdue.split()[0].strip() if Interest_Overdue else '',
            'Charges_Overdue': Charges_Overdue,
            'Last_Payment_Date': Last_Payment_Date,
            'Last_Payment_Amount': Last_Payment_Amount,
            'Bucket': Bucket,
            # 'Account_no': Account_no,
            'Account_no': f"'{Account_no}" if Account_no else '',
            'cif_cid': cif_cid if cif_cid else '',
            'scrape_date': str(scrape_date)
        }
        return data

    def parse1(self, response, meta):
        import quopri

        mhtml_raw = meta['raw_mhtml']

        # Step 1: Parse as email
        msg = message_from_string(mhtml_raw, policy=default)

        html_body = None
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == 'text/html':
                    raw_html = part.get_payload(decode=True)  # bytes
                    html_body = quopri.decodestring(raw_html).decode('utf-8', errors='ignore')
                    break
        else:
            if msg.get_content_type() == 'text/html':
                raw_html = msg.get_payload(decode=True)
                html_body = quopri.decodestring(raw_html).decode('utf-8', errors='ignore')

        if not html_body:
            self.logger.warning("No HTML content found")
            return

        import re

        # Step 2: parse with lxml
        parser = html.fromstring(html_body)

        Account_no = parser.xpath("//div[@id='selectedLoanNo_chosen']//span//text()")
        Account_no = ''.join(Account_no).strip()
        # print(parser.xpath("//div[@id='selectedLoanNo_chosen']//span//text()"))

        Product_Type = parser.xpath("//b[text()='Product Type']//parent::div/div//text()") or parser.xpath('//div[@code="cardType"]//div[2]//text()')
        Product_Type = ''.join(Product_Type).replace('\xa0', '').strip()

        Application_ID = parser.xpath('//div[@code="applicationId"]//div[2]//text()')
        Application_ID = ''.join(Application_ID).strip()

        Net_Disbursal_Amount = parser.xpath("//b[contains(text(), 'Net Disbursal Amount')]//parent::div/div//text()")
        Net_Disbursal_Amount = ''.join(Net_Disbursal_Amount).strip()

        product = parser.xpath("//b[text()='Product']//parent::div/div//text()") or parser.xpath('//div[@code="product"]//div[2]//text()')
        product = ''.join(product).replace('\xa0', '').replace('&nbsp;', '').strip()

        Branch_Code = parser.xpath("//b[contains(text(), 'Branch Code')]//parent::div/div//text()") or parser.xpath('//div[@code="branchCode"]//div[2]//text()')
        Branch_Code = ''.join(Branch_Code).strip()

        Frequency = parser.xpath("//b[contains(text(), 'Frequency')]//parent::div/div//text()")
        Frequency = ''.join(Frequency).strip()

        Scheme = parser.xpath("//b[contains(text(), 'Scheme')]//parent::div/div//text()")
        Scheme = ''.join(Scheme).strip()

        Loan_Disbursed = parser.xpath("//b[contains(text(), 'Loan Disbursed')]//parent::div/div//text()") or parser.xpath('//div[@code="loanDisbursed"]//div[2]//text()')
        Loan_Disbursed = ''.join(Loan_Disbursed).strip()

        Next_Due_Amount = parser.xpath("//b[contains(text(), 'Next Due Amount')]//parent::div/div//text()") or parser.xpath('//div[@code="nextDueAmount"]//div[2]//text()') # nextDueAmount
        Next_Due_Amount = ''.join(Next_Due_Amount).strip()
        

        Installment_Amount = parser.xpath("//b[contains(text(), 'Installment Amount')]//parent::div/div//text()")
        Installment_Amount = ''.join(Installment_Amount).strip()

        Billed_Amount = parser.xpath("//b[contains(text(), 'Billed Amount')]//parent::div/div//text()")
        Billed_Amount = ''.join(Billed_Amount).strip()

        Balance_O_S = parser.xpath("//b[contains(text(), 'Balance O/S')]//parent::div/div//text()") or parser.xpath('//div[@code="balanceoutstanding"]//div[2]//text()')
        Balance_O_S = ''.join(Balance_O_S).strip()

        
        Principal_Outstanding = ''.join(
            parser.xpath("//b[text()='Principal Outstanding ']//parent::div//div//text()")
        ).strip()

        Interest_Outstanding = ''.join(
            parser.xpath("//b[text()='Interest Outstanding ']//parent::div/div//text()")
        ).strip()

        Next_Due_Date = parser.xpath("//b[contains(text(), 'Next Due Date')]//parent::div/div//text()") or parser.xpath('//div[@code="nextDueDate"]//div[2]//text()')
        Next_Due_Date = ''.join(Next_Due_Date).strip()


        Interest_Type = ''.join(
            parser.xpath("//b[contains(text(), 'Interest Type')]//parent::div/div//text()")
        ).strip()

        Interest_Rate_percentage = parser.xpath("//b[contains(text(), 'Interest Rate')]//parent::div/div//text()") or parser.xpath('//div[@code="interestrate"]//div[2]//text()')
        Interest_Rate_percentage = ''.join(Interest_Rate_percentage).strip()

        
        Tenure = ''.join(
            parser.xpath("//b[contains(text(), 'Tenure')]//parent::div/div//text()")
        ).strip()

        Account_Status = ''.join(
            parser.xpath("//b[contains(text(), 'Account Status')]//parent::div/div//text()")
        ).strip()

        Non_Instrument_Based_Repayment = ''.join(
            parser.xpath("//b[contains(text(), 'Non Instrument Based Repayment')]//parent::div/div//text()")
        ).strip()

        Last_Bounce_Date = ''.join(
            parser.xpath("//b[contains(text(), 'Last Bounce Date')]//parent::div/div//text()")
        ).strip()

        Written_Off_Amount = ''.join(
            parser.xpath("//b[contains(text(), 'Written Off Amount')]//parent::div/div//text()")
        ).strip()

        Total_Repayment_Amount = ''.join(
            parser.xpath("//b[contains(text(), 'Total Repayment Amount')]//parent::div/div//text()")
        ).strip()

        Maturity_Date = ''.join(
            parser.xpath("//b[contains(text(), 'Maturity Date')]//parent::div/div//text()")
        ).strip()

        Disbursal_Date = ''.join(
            parser.xpath("//b[contains(text(), 'Disbursal Date')]//parent::div/div//text()")
        ).strip()

        
        Principal_Outstanding_Amount = parser.xpath("//b[text()='Principal Outstanding Amount']//parent::div/div//text()") or parser.xpath('//div[@code="principalOs"]//div[2]//text()')
        Principal_Outstanding_Amount = ''.join(Principal_Outstanding_Amount).strip()

        
        Principal_Overdue = parser.xpath("//b[contains(text(), 'Principal Overdue')]//parent::div/div//text()") or parser.xpath('//div[@code="principalOd"]//div[2]//text()')
        Principal_Overdue = ''.join(Principal_Overdue).strip()

        Interest_Outstanding_Amount = parser.xpath("//b[contains(text(), 'Interest Outstanding Amount')]//parent::div/div//text()") or parser.xpath('//div[@code="interestOs"]//div[2]//text()')
        Interest_Outstanding_Amount = ''.join(Interest_Outstanding_Amount).strip()

        # lateFees
        # parser.xpath('//div[@code="lateFees"]//div[2]//text()')
        Interest_Overdue_Amount = parser.xpath("//b[contains(text(), 'Interest Overdue Amount')]//parent::div/div//text()") or parser.xpath('//div[@code="interestOd"]//div[2]//text()')
        Interest_Overdue_Amount = ''.join(Interest_Overdue_Amount).strip()

        Late_Fees = parser.xpath("//b[contains(text(), 'Late Fees')]//parent::div/div//text()") or parser.xpath('//div[@code="lateFees"]//div[2]//text()')
        Late_Fees = ''.join(Late_Fees).strip()

        cif_cid = ''.join(
            parser.xpath("//label[contains(text(), 'CIF#')]//parent::div/text()")
        )
        cif_cid = ''.join(
            [i.strip() for i in cif_cid if i.strip()]
        ) if cif_cid else ''
        data = {
            'Account_no': f"'{Account_no}" if Account_no else '',
            # 'customer_name': customer_name if customer_name else '',
            'Product_Type': Product_Type if Product_Type else '',
            'Application_ID': Application_ID if Application_ID else '',
            'Net_Disbursal_Amount': Net_Disbursal_Amount if Net_Disbursal_Amount else '',
            'product': product if product else '',
            'Branch_Code': Branch_Code if Branch_Code else '',
            'Frequency': Frequency if Frequency else '',
            'Scheme': Scheme if Scheme else '',
            'Loan_Disbursed': Loan_Disbursed if Loan_Disbursed else '',
            'Next_Due_Amount': Next_Due_Amount if Next_Due_Amount else '',
            'Installment_Amount': Installment_Amount if Installment_Amount else '',
            'Billed_Amount': Billed_Amount if Billed_Amount else '',
            'Balance_O_S': Balance_O_S if Balance_O_S else '',
            'Principal_Outstanding': Principal_Outstanding if Principal_Outstanding else '',
            'Interest_Outstanding': Interest_Outstanding if Interest_Outstanding else '',
            'Next_Due_Date': Next_Due_Date if Next_Due_Date else '',
            'Interest_Type': Interest_Type if Interest_Type else '',
            'Interest_Rate_percentage': Interest_Rate_percentage if Interest_Rate_percentage else '',
            'Tenure': Tenure if Tenure else '',
            'Account_Status': Account_Status if Account_Status else '',
            'Non_Instrument_Based_Repayment': Non_Instrument_Based_Repayment if Non_Instrument_Based_Repayment else '',
            'Last_Bounce_Date': Last_Bounce_Date if Last_Bounce_Date else '',
            'Written_Off_Amount': Written_Off_Amount if Written_Off_Amount else '',
            'Total_Repayment_Amount': Total_Repayment_Amount if Total_Repayment_Amount else '',
            'Maturity_Date': Maturity_Date if Maturity_Date else '',
            'Disbursal_Date': Disbursal_Date if Disbursal_Date else '',
            'Principal_Outstanding_Amount': Principal_Outstanding_Amount if Principal_Outstanding_Amount else '',
            'Principal_Overdue': Principal_Overdue.split()[0].strip() if Principal_Overdue else '',
            'Interest_Outstanding_Amount': Interest_Outstanding_Amount if Interest_Outstanding_Amount else '',
            'Interest_Overdue_Amount': Interest_Overdue_Amount if Interest_Overdue_Amount else '',
            'Late_Fees': Late_Fees if Late_Fees else '',
            'cif_cid': cif_cid.replace('=09', '') if cif_cid else '',
        }
        # yield Product(**data)
        return data

    def generate_items(self):
        items = []
        if self.finn_uae_list and self.finn_uae_sub_list:
            for main in self.finn_uae_list:
                for sub in self.finn_uae_sub_list:
                    if main.get('Account_no') in sub.get('Account_no'):
                        combined = {**main, **sub}
                        items.append(combined)
                        # return
                        # brea
        return items  # Always returns an empty list if no matches