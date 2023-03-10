from datetime import datetime
from ftplib import FTP


class DATASUS:
    name = "DATASUS"
    ftp_address = "ftp.datasus.gov.br"
    repositories = {
        "SIHSUS": {
            "addresses": [
                "/dissemin/publicos/SIHSUS/199201_200712/Dados",
                "/dissemin/publicos/SIHSUS/200801_/Dados",
            ],
            "tables": ["RD", "SP"],
            "date_min": datetime(1992, 1, 1).date(),
        },
        "SIASUS": {
            "addresses": [
                "/dissemin/publicos/SIASUS/199407_200712/Dados",
                "/dissemin/publicos/SIASUS/200801_/Dados",
            ],
            "tables": ["PA"],
            "date_min": datetime(1994, 7, 1).date(),
        },
    }

    def __init__(self, path, storage):
        self._path = path
        self._storage = storage
        self._repository = None
        self._table = None
        self._date_start = None
        self._date_end = None
        self.states = []
        return

    def _execute_pipeline(self, repository, table, date_start, date_end, states):
        self._repository = repository
        self._table = table
        self._date_start = date_start
        self._date_end = date_end
        self.states = states
        files_to_download = self._get_files_to_download()
        files_transformed = []
        for file in files_to_download:
            file_transformed = self._download_transform_file(file)
            if file_transformed is not None:
                files_transformed.append(file_transformed)
        return files_transformed

    def _get_files_to_download(self):
        files = []
        temp_files = []
        for repository_address in self._repository["addresses"]:
            ftp = FTP(self.ftp_address)
            ftp.login()
            ftp.dir(repository_address, temp_files.append)
            ftp.close()
            for file in temp_files:
                file = file.split()
                file.append(repository_address)
                files.append(file)
            temp_files = []
        files = [self._get_metadata_from_file(file) for file in files]
        files = [file for file in files if file is not None]
        return files

    def _get_metadata_from_file(self, file):
        original_name = file[3]
        repository_address = file[4]
        name = original_name.upper()

        path = f"{repository_address}/{original_name}"
        name_extension = name.split(".")[1].lower()
        if name_extension != "dbc":
            return None

        table = name[0:2]
        state_initials = name[2:4]
        year = int(name[4:6])
        year = 1900 + year if year >= 30 else 2000 + year
        month = name[6:8]
        month = int(month) if month.isdigit() else 1
        date_ref = datetime(year=year, month=month, day=1).date()

        if (
            self._date_start <= date_ref <= self._date_end
            and state_initials in self.states
            and table == self._table
        ):
            return path
        else:
            return None

    def _download_transform_file(self, file):
        file_downloaded = self._download_dbc(file)
        self._transform_dbf(file_downloaded)
        self._transform_csv(file_downloaded)
        self._transform_parquet(file_downloaded)
        return

    def _download_dbc(self, file):
        ftp = FTP(self.ftp_address)
        ftp.login()
        with open(file, "wb") as file:
            ftp.retrbinary("RETR", file.write)
        ftp.close()
        return

    def _transform_dbf(self, file):
        return

    def _transform_csv(self, file):
        return

    def _transform_parquet(self, file):
        return


sources = {DATASUS.name: DATASUS}
