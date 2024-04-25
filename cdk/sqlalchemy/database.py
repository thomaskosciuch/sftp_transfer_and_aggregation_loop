from sqlalchemy import Table, Column, String, DATE, MetaData, Integer, DATETIME, String, DECIMAL, Date, create_engine
from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql.sqltypes import DECIMAL

from sqlalchemy.orm import scoped_session, sessionmaker, declarative_base
import urllib.parse
from os import environ, getenv

rds_host  = environ['prod_NBIN_RDS_HOST']
username = environ['prod_NBIN_SQL_USERNAME']
password = urllib.parse.quote_plus(environ['prod_NBIN_SQL_PASSWORD'])
db_name = getenv('prod_NBIN_DN_NAME', 'nbinDataFeed')
environment='production'

rds_host  = "criwycoituxs.ca-central-1.rds.amazonaws.com"
username = "nbinFeed"
password = urllib.parse.quote_plus("QWe@lth!1")
db_name = "nbinDataFeed"

engine = create_engine(f'mysql+pymysql://{username}:{password}@qw-{environment}.{rds_host}/{db_name}')

Base = declarative_base()
metadata = MetaData()
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
query = db_session.query
                                         

metadata_obj = MetaData()

class IbmsmProcessLog(Base):
    __table__ = Table(
        "ibmsm_process_log",
        metadata_obj,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('filename', String(255)),
        Column('process_date', DATE),
        Column('mtime', DATETIME)
    )

class Security(Base):
    """
    To prevent splitting table into two; delected some columns as POC
    """
    
    __tablename__ = 'securities'

    id = Column(Integer, primary_key=True)
    # TI_Desc_1 = Column(String(255))
    TI_Short_Desc = Column(String(255))
    TI_Alternate_Short_Desc = Column(String(255))
    # TI_Alternate_Desc_1 = Column(String(255))
    TI_Type = Column(String(2))
    TI = Column(Integer)
    IND_ANN_DIV = Column(Integer)
    # TI_Desc_2 = Column(String(255))
    # TI_Alternate_Desc_2 = Column(String(255))
    Last_Maint_Date = Column(Date)
    Issuer = Column(String(255))
    TI_Alternate_ID = Column(String(255))
    # Data_Source = Column(String(255))
    TI_Face_Value = Column(DECIMAL)
    TI_Basis_Point = Column(DECIMAL)
    # TI_User_Desc = Column(String(255))
    TI_Issue_Price = Column(DECIMAL)
    TI_Issue_Date = Column(Date)
    # Bankruptcy_Code = Column(String(255))
    Income_Profile_Base_Date = Column(Date)
    TI_Mat_Date = Column(Date)
    TI_Pickup_Date = Column(Date)
    CALLABLE_FLAG = Column(String(255))
    CCPC = Column(DECIMAL)
    TI_CUSIP = Column(String(255))
    TI_Symbol = Column(String(255))
    CALL_DATE = Column(Date)
    # Commission_Payment_Code = Column(String(255))
    Country_of_incorporation = Column(String(3))
    CONVERTABLE_FLAG = Column(String(2))
    # Compound_Interest_Type = Column(String(255))
    # Compound_Interest_Rate = Column(DECIMAL)
    # TI_Deletion_Sw = Column(String(255))
    CONVERSION_DATE = Column(Date)
    TI_Alternate_Conv_Factor = Column(DECIMAL)
    # Defunct_Code = Column(String(255))
    # Deemed_Currency = Column(String(255))
    DEEMED_VALUE = Column(DECIMAL)
    Duplicate_Security_Flag = Column(String(1))
    TI_Note = Column(DECIMAL)
    BBS_ELIGIBLE_FLAG = Column(Integer)
    DCS_ELIGIBLE_FLAG = Column(Integer)
    DTC_ELIGIBLE_FLAG = Column(Integer)
    EURO_ELIGIBLE_FLAG = Column(Integer)
    FGN_CONTENT_ELIG = Column(String(1))
    TI_ISIN_Check_Digit = Column(Integer)
    English_Description = Column(String(255))
    RRSP_ELIGIBLE_FLAG = Column(Integer)
    Extra_Seg_Id = Column(String(1))
    WCCC_ELIGIBLE_FLAG = Column(Integer)
    Exersized_Base_Quantity = Column(DECIMAL)
    EXTENDABLE_FLAG = Column(String(1))
    FGN_CONTENT_EFF_DATE = Column(Date)
    French_Description = Column(String(255))
    TI_Class = Column(String(3))
    HEDGE_FUND_FLAG = Column(String(1))
    TI_2nd_Market = Column(String(255))
    TI_3rd_Market = Column(String(255))
    TI_4th_Market = Column(String(255))
    TI_5th_Market = Column(String(255))
    TI_6th_Market = Column(String(255))
    Income_Activity_Type = Column(String(255))
    Income_Profile_Currency = Column(String(255))
    Income_Description = Column(String(255))
    Income_Freq = Column(Integer)
    Income_Profile_Rate = Column(DECIMAL)
    ISIN_NO = Column(String(255))
    JSCC_ELIGIBLE_FLAG = Column(String(255))
    LSI_FUND_FLAG = Column(String(255))
    Last_UPD_Date = Column(Date)
    TI_Primary_Market = Column(String(255))
    MBS_Pool_Num = Column(String(255))
    MF_Load_Code = Column(String(255))
    OFF_MEMORADUM_FLAG = Column(String(255))
    PROSPECTUS_FLAG = Column(String(255))
    UOM_Multiply_Divide_Indic = Column(String(255))
    TI_Alternate_TI_Class = Column(String(255))
    TI_Alternate_TI_Type = Column(String(255))
    TI_ISIN_Country_Code = Column(String(255))
    Option_Eligibility_Code = Column(String(1))
    TI_Parent_Type = Column(String(255))
    Redeemable_Flag = Column(String(1))
    RETRACTABLE_FLAG = Column(String(1))
    REM_PRIN_FACTOR = Column(Integer)
    RRSP_Known_Indicator = Column(Integer)
    Security_Sub_Class = Column(String(255))
    SPEC_INFORMATION = Column(String(255))
    SPEC_MARGIN_RATE = Column(DECIMAL)
    Source = Column(String(255))
    Source_Type = Column(String(255))
    Strike_Condition_Code = Column(String(255))
    Strike_Price_Currency = Column(String(255))
    Legal_Start_Date = Column(Date)
    STRIKE_PRICE = Column(DECIMAL)
    Default_Settle_Type = Column(String(255))
    Normal_Trade_Currency = Column(String(255))
    Trailer_Count = Column(String(255))
    Global_Trading_Status = Column(String(255))
    Tax_Status_Code = Column(String(255))
    TI_Underlying_CDN_Price = Column(DECIMAL)
    Effective_Date_of_UOM = Column(Date)
    Unit_of_measure = Column(DECIMAL)
    Update_Code = Column(String(255))
    TI_Underlying_Sec_No = Column(String(255))
    TI_Underlying_USD_Price = Column(DECIMAL)
    WITHHOLDING_TAX_CDE = Column(String(255))
    Mkt_Price_Date = Column(Date)
    Market_Price_Close_Yield = Column(DECIMAL)
    TI_Alternate_Id_2 = Column(String(255))
    Market_Price_Bid = Column(DECIMAL)
    Market_Price_Ask = Column(DECIMAL)
    Market_Price_Close = Column(DECIMAL)
    Market_Price_Low = Column(DECIMAL)
    Market_Price_High = Column(DECIMAL)
    Conversion_Ind = Column(String(255))
    Market_Price_Source = Column(String(255))
    House_Broker_Id = Column(String(255))
    House_Business_Unit = Column(String(255))
    Market = Column(String(255))
    Default_Price_Ind = Column(String(255))
    EOD_Price_Type = Column(String(255))
    Price_Currency = Column(String(255))
    Last_Maint_Time = Column(Date)
    Last_Maint_User = Column(String(255))
    Last_Maint_Job = Column(String(255))
    TI_Income_Date = Column(Date)
    Last_Maint_Type = Column(String(255))
    TI_Income_Rate = Column(DECIMAL)
    TI_Income_Record_Date = Column(Date)
    TI_Income_Seq = Column(Integer)
    Conditional_Code = Column(String(255))
    Description = Column(String(255))
    TI_Income_Amt = Column(DECIMAL)
    Distribution_Source = Column(String(255))
    Ex_Dividend_Date = Column(Date)
    Income_End_Date = Column(Date)
    Income_Frequency = Column(Integer)
    Interest_Factor = Column(DECIMAL)
    TI_Income_Currency = Column(String(255))
    Non_Dividend_Date = Column(Date)
    Payment_Method_Code = Column(String(255))
    Principle_Factor = Column(DECIMAL)
    # Payment_Frequency = Column(Integer)
    # Share_Base = Column(DECIMAL)
    # Tax_Class_Code = Column(String(255))
    # Income_Type = Column(String(255))
    # Withholding_Tax_Code = Column(String(255))
    # ACCRUED_INT_METHOD_1 = Column(String(255))
    # ACCRUED_INT_METHOD_2 = Column(String(255))
    # TI_IncDf_Income_Amt = Column(DECIMAL)
    # TI_IncDf_Income_Rate = Column(DECIMAL)
    # COMPOUND_BOND_INDIC = Column(String(255))
    # DBRS_bond_effective_date = Column(Date)
    # DBRS_bond_rating = Column(String(255))
    # DBRS_bond_rating_source = Column(String(255))
    # TI_IncDf_Income_Freq = Column(Integer)
    # TI_IncDf_Income_Type = Column(String(255))
    # COMPOUND_BOND_INT_RATE = Column(DECIMAL)
    # COMPOUND_FREQ = Column(Integer)
    # BOND_COUPON_DATE_10 = Column(Date)
    # BOND_COUPON_DATE_11 = Column(Date)
    # BOND_COUPON_DATE_12 = Column(Date)
    # BOND_COUPON_DATE_1 = Column(Date)
    # BOND_COUPON_DATE_2 = Column(Date)
    # BOND_COUPON_DATE_3 = Column(Date)
    # BOND_COUPON_DATE_4 = Column(Date)
    # BOND_COUPON_DATE_5 = Column(Date)
    # BOND_COUPON_DATE_6 = Column(Date)
    # BOND_COUPON_DATE_7 = Column(Date)
    # BOND_COUPON_DATE_8 = Column(Date)
    # BOND_COUPON_DATE_9 = Column(Date)
    # Default_Bond_Rating = Column(String(255))
    # TI_IncDf_Dated_Date = Column(Date)
    # TI_IncDf_First_Coupon_Dat = Column(Date)
    # DECIMALING_RATE_INDIC = Column(String(255))
    # Income_Currency = Column(String(255))
    # Interest_discount_indicat = Column(String(255))
    # Reissue_Currency = Column(String(255))
    # Reissue_Price = Column(DECIMAL)
    # Bond_Registration_Status = Column(String(255))
    # REISSUE_DATE = Column(Date)
    # S_P_bond_rating_EFF_DATE = Column(Date)
    # S_P_bond_rating = Column(String(255))
    # S_P_bond_rating_source = Column(String(255))
    # USE_DATED_RATES_INDIC = Column(String(255))
    
ibmsm_csv_mapping = {
    # 'TI Desc 1': 'TI_Desc_1',
    'TI Short Desc': 'TI_Short_Desc',
    'TI Alternate Short Desc': 'TI_Alternate_Short_Desc',
    # 'TI Alternate Desc 1': 'TI_Alternate_Desc_1',
    'TI Type': 'TI_Type',
    'TI': 'TI',
    'IND ANN DIV': 'IND_ANN_DIV',
    # 'TI Desc 2': 'TI_Desc_2',
    # 'TI Alternate Desc 2': 'TI_Alternate_Desc_2',
    'Last Maint Date': 'Last_Maint_Date',
    'Issuer': 'Issuer',
    'TI Alternate ID': 'TI_Alternate_ID',
    # 'Data Source': 'Data_Source', #always IBM
    'TI Face Value': 'TI_Face_Value',
    'TI Basis Point': 'TI_Basis_Point',
    # 'TI User Desc': 'TI_User_Desc',
    'TI Issue Price': 'TI_Issue_Price',
    'TI Issue Date': 'TI_Issue_Date',
    # 'Bankruptcy Code': 'Bankruptcy_Code',
    'Income Profile Base Date': 'Income_Profile_Base_Date',
    'TI Mat Date': 'TI_Mat_Date',
    'TI Pickup Date': 'TI_Pickup_Date',
    'CALLABLE FLAG': 'CALLABLE_FLAG',
    'CCPC': 'CCPC',
    'TI CUSIP': 'TI_CUSIP',
    'TI Symbol': 'TI_Symbol',
    'CALL DATE': 'CALL_DATE',
    # 'Commission Payment Code': 'Commission_Payment_Code',
    'Country of incorporation': 'Country_of_incorporation',
    'CONVERTABLE FLAG': 'CONVERTABLE_FLAG',
    # 'Compound Interest Type': 'Compound_Interest_Type',
    # 'Compound Interest Rate': 'Compound_Interest_Rate',
    # 'TI Deletion Sw': 'TI_Deletion_Sw',
    'CONVERSION DATE': 'CONVERSION_DATE',
    'TI Alternate Conv Factor': 'TI_Alternate_Conv_Factor',
    # 'Defunct Code': 'Defunct_Code',
    # 'Deemed Currency': 'Deemed_Currency',
    'DEEMED VALUE': 'DEEMED_VALUE',
    'Duplicate Security Flag': 'Duplicate_Security_Flag',
    'TI Note': 'TI_Note',
    'BBS ELIGIBLE FLAG': 'BBS_ELIGIBLE_FLAG',
    'DCS ELIGIBLE FLAG': 'DCS_ELIGIBLE_FLAG',
    'DTC ELIGIBLE FLAG': 'DTC_ELIGIBLE_FLAG',
    'EURO ELIGIBLE FLAG': 'EURO_ELIGIBLE_FLAG',
    'FGN CONTENT ELIG': 'FGN_CONTENT_ELIG',
    'TI ISIN Check Digit': 'TI_ISIN_Check_Digit',
    'English Description': 'English_Description',
    'RRSP ELIGIBLE FLAG': 'RRSP_ELIGIBLE_FLAG',
    'Extra Seg Id': 'Extra_Seg_Id',
    'WCCC ELIGIBLE FLAG': 'WCCC_ELIGIBLE_FLAG',
    'Exersized Base Quantity': 'Exersized_Base_Quantity',
    'EXTENDABLE FLAG': 'EXTENDABLE_FLAG',
    'FGN CONTENT EFF  DATE': 'FGN_CONTENT_EFF_DATE',
    'French Description': 'French_Description',
    'TI Class': 'TI_Class',
    'HEDGE FUND FLAG': 'HEDGE_FUND_FLAG',
    'TI 2nd Market': 'TI_2nd_Market',
    'TI 3rd Market': 'TI_3rd_Market',
    'TI 4th Market': 'TI_4th_Market',
    'TI 5th Market': 'TI_5th_Market',
    'TI 6th Market': 'TI_6th_Market',
    'Income Activity Type': 'Income_Activity_Type',
    'Income Profile Currency': 'Income_Profile_Currency',
    'Income Description': 'Income_Description',
    'Income Freq': 'Income_Freq',
    'Income Profile Rate': 'Income_Profile_Rate',
    'ISIN NO': 'ISIN_NO',
    'JSCC ELIGIBLE FLAG': 'JSCC_ELIGIBLE_FLAG',
    'LSI FUND FLAG': 'LSI_FUND_FLAG',
    'Last UPD Date': 'Last_UPD_Date',
    'TI Primary Market': 'TI_Primary_Market',
    'MBS Pool Num': 'MBS_Pool_Num',
    'MF Load Code': 'MF_Load_Code',
    'OFF MEMORADUM FLAG': 'OFF_MEMORADUM_FLAG',
    'PROSPECTUS FLAG': 'PROSPECTUS_FLAG',
    'UOM Multiply Divide Indic': 'UOM_Multiply_Divide_Indic',
    'TI Alternate TI Class': 'TI_Alternate_TI_Class',
    'TI Alternate TI Type': 'TI_Alternate_TI_Type',
    'TI ISIN Country Code': 'TI_ISIN_Country_Code',
    'Option Eligibility Code': 'Option_Eligibility_Code',
    'TI Parent Type': 'TI_Parent_Type',
    'Redeemable Flag': 'Redeemable_Flag',
    'RETRACTABLE FLAG': 'RETRACTABLE_FLAG',
    'REM PRIN FACTOR': 'REM_PRIN_FACTOR',
    'RRSP Known Indicator': 'RRSP_Known_Indicator',
    'Security Sub Class': 'Security_Sub_Class',
    'SPEC INFORMATION': 'SPEC_INFORMATION',
    'SPEC MARGIN RATE': 'SPEC_MARGIN_RATE',
    'Source': 'Source',
    'Source Type': 'Source_Type',
    'Strike Condition Code': 'Strike_Condition_Code',
    'Strike Price Currency': 'Strike_Price_Currency',
    'Legal Start Date': 'Legal_Start_Date',
    'STRIKE PRICE': 'STRIKE_PRICE',
    'Default Settle Type': 'Default_Settle_Type',
    'Normal Trade Currency': 'Normal_Trade_Currency',
    'Trailer Count': 'Trailer_Count',
    'Global Trading Status': 'Global_Trading_Status',
    'Tax Status Code': 'Tax_Status_Code',
    'TI Underlying CDN Price': 'TI_Underlying_CDN_Price',
    'Effective Date of UOM': 'Effective_Date_of_UOM',
    'Unit of measure': 'Unit_of_measure',
    'Update Code': 'Update_Code',
    'TI Underlying Sec No': 'TI_Underlying_Sec_No',
    'TI Underlying USD Price': 'TI_Underlying_USD_Price',
    'WITHHOLDING TAX CDE': 'WITHHOLDING_TAX_CDE',
    'Mkt Price Date': 'Mkt_Price_Date',
    'Market Price Close Yield': 'Market_Price_Close_Yield',
    'TI Alternate Id': 'TI_Alternate_Id_2',
    'Market Price Bid': 'Market_Price_Bid',
    'Market Price Ask': 'Market_Price_Ask',
    'Market Price Close': 'Market_Price_Close',
    'Market Price Low': 'Market_Price_Low',
    'Market Price High': 'Market_Price_High',
    'Conversion Ind': 'Conversion_Ind',
    'Market Price Source': 'Market_Price_Source',
    'House Broker Id': 'House_Broker_Id',
    'House Business Unit': 'House_Business_Unit',
    'Market': 'Market',
    'Default Price Ind': 'Default_Price_Ind',
    'EOD Price Type': 'EOD_Price_Type',
    'Price Currency': 'Price_Currency',
    'Last Maint Time': 'Last_Maint_Time',
    'Last Maint User': 'Last_Maint_User',
    'Last Maint Job': 'Last_Maint_Job',
    'TI Income Date': 'TI_Income_Date',
    'Last Maint Type': 'Last_Maint_Type',
    'TI Income Rate': 'TI_Income_Rate',
    'TI Income Record Date': 'TI_Income_Record_Date',
    'TI Income Seq': 'TI_Income_Seq',
    'Conditional Code': 'Conditional_Code',
    'Description': 'Description',
    'TI Income Amt': 'TI_Income_Amt',
    'Distribution Source': 'Distribution_Source',
    'Ex-Dividend Date': 'Ex_Dividend_Date',
    'Income End Date': 'Income_End_Date',
    'Income Frequency': 'Income_Frequency',
    'Interest Factor': 'Interest_Factor',
    'TI Income Currency': 'TI_Income_Currency',
    'Non-Dividend Date': 'Non_Dividend_Date',
    'Payment Method Code': 'Payment_Method_Code',
    # 'Principle Factor': 'Principle_Factor',
    # 'Payment Frequency': 'Payment_Frequency',
    # 'Share Base': 'Share_Base',
    # 'Tax Class Code': 'Tax_Class_Code',
    # 'Income Type': 'Income_Type',
    # 'Withholding Tax Code': 'Withholding_Tax_Code',
    # 'ACCRUED INT METHOD 1': 'ACCRUED_INT_METHOD_1',
    # 'ACCRUED INT METHOD 2': 'ACCRUED_INT_METHOD_2',
    # 'TI IncDf Income Amt': 'TI_IncDf_Income_Amt',
    # 'TI IncDf Income Rate': 'TI_IncDf_Income_Rate',
    # 'COMPOUND BOND INDIC': 'COMPOUND_BOND_INDIC',
    # 'DBRS bond effective date': 'DBRS_bond_effective_date',
    # 'DBRS bond rating': 'DBRS_bond_rating',
    # 'DBRS bond rating source': 'DBRS_bond_rating_source',
    # 'TI IncDf Income Freq': 'TI_IncDf_Income_Freq',
    # 'TI IncDf Income Type': 'TI_IncDf_Income_Type',
    # 'COMPOUND BOND INT RATE': 'COMPOUND_BOND_INT_RATE',
    # 'COMPOUND FREQ': 'COMPOUND_FREQ',
    # 'BOND COUPON DATE 10': 'BOND_COUPON_DATE_10',
    # 'BOND COUPON DATE 11': 'BOND_COUPON_DATE_11',
    # 'BOND COUPON DATE 12': 'BOND_COUPON_DATE_12',
    # 'BOND COUPON DATE 1': 'BOND_COUPON_DATE_1',
    # 'BOND COUPON DATE 2': 'BOND_COUPON_DATE_2',
    # 'BOND COUPON DATE 3': 'BOND_COUPON_DATE_3',
    # 'BOND COUPON DATE 4': 'BOND_COUPON_DATE_4',
    # 'BOND COUPON DATE 5': 'BOND_COUPON_DATE_5',
    # 'BOND COUPON DATE 6': 'BOND_COUPON_DATE_6',
    # 'BOND COUPON DATE 7': 'BOND_COUPON_DATE_7',
    # 'BOND COUPON DATE 8': 'BOND_COUPON_DATE_8',
    # 'BOND COUPON DATE 9': 'BOND_COUPON_DATE_9',
    # 'Default Bond Rating': 'Default_Bond_Rating',
    # 'TI IncDf Dated Date': 'TI_IncDf_Dated_Date',
    # 'TI IncDf First Coupon Dat': 'TI_IncDf_First_Coupon_Dat',
    # 'DECIMALING RATE INDIC': 'DECIMALING_RATE_INDIC',
    # 'Income Currency': 'Income_Currency',
    # 'Interest discount indicat': 'Interest_discount_indicat',
    # 'Reissue Currency': 'Reissue_Currency',
    # 'Reissue Price': 'Reissue_Price',
    # 'Bond Registration Status': 'Bond_Registration_Status',
    # 'REISSUE DATE': 'REISSUE_DATE',
    # 'S&P bond rating EFF DATE': 'S_P_bond_rating_EFF_DATE',
    # 'S&P bond rating': 'S_P_bond_rating',
    # 'S&P bond rating source': 'S_P_bond_rating_source',
    # 'USE DATED RATES INDIC': 'USE_DATED_RATES_INDIC'
}
