from aws_cdk import aws_ssm

from typing import TypedDict

class SentryGroup(TypedDict):
    SENTRY_DSN: str
    NAME: str

class SftpGroup(TypedDict):
    SFTP_NBIN_USER: str
    SFTP_NBIN_PASSWORD: str
    SFTP_NBIN_HOST: str

class SqlGroup(TypedDict):
    prod_NBIN_SQL_USERNAME: str
    prod_NBIN_SQL_PASSWORD: str
    prod_NBIN_RDS_HOST: str
    prod_NBIN_DN_NAME: str


class IbmsmEnvVars(SentryGroup, SqlGroup, SftpGroup):
    pass

def ibmsm_env_vars(self, NAME:str) -> IbmsmEnvVars:
    ssm_alias = aws_ssm.StringParameter.value_for_string_parameter
    sentry_dsn = ssm_alias(self, "LAMBDA_SENTRY_DSN")
    sql_username = ssm_alias(self, "prod_NBIN_SQL_USERNAME")
    sql_password = ssm_alias(self, "prod_NBIN_SQL_PASSWORD")
    sql_host = ssm_alias(self, "prod_NBIN_RDS_HOST")
    sql_database = ssm_alias(self, "prod_NBIN_DN_NAME")
    user = ssm_alias(self, "SFTP_NBIN_USER")
    host_url = ssm_alias(self, "SFTP_NBIN_HOST")
    password = ssm_alias(self, "SFTP_NBIN_PASSWORD")
    return {
        "NAME": NAME,
        "SENTRY_DSN": sentry_dsn,
        "prod_NBIN_SQL_USERNAME": sql_username,
        "prod_NBIN_SQL_PASSWORD": sql_password,
        "prod_NBIN_RDS_HOST": sql_host,
        "prod_NBIN_DN_NAME": sql_database,
        "SFTP_NBIN_USER": user,
        "SFTP_NBIN_PASSWORD": password,
        "SFTP_NBIN_HOST": host_url,
    }
