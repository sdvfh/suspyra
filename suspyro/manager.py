from datetime import datetime

from .sources import sources
from .utils import build_spark, get_path


class Manager:

    _states = [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    ]

    def __init__(self, storage):
        self.storage = storage
        self.spark = build_spark()
        self._path = get_path()
        return

    def link(self, source, repository, table, date_start, date_end, states):
        source = self._valid_source(source)
        repository = self._valid_repository(source, repository)
        date_start, date_end = self._valid_dates(repository, date_start, date_end)
        states = self._valid_states(states)
        files = source._execute_pipeline(
            repository, table, date_start, date_end, states
        )
        self.spark.read.format("parquet").load(files).createOrReplaceTempView(table)
        return

    def _valid_source(self, source):
        if source not in sources.keys():
            raise ValueError(
                f'Source "{source!r}" not found. Available sources: {list(sources.keys())}.'
            )
        return sources[source](self._path, self.storage)

    @staticmethod
    def _valid_repository(source, repository):
        repositories = source.repositories
        if repository not in repositories.keys():
            raise ValueError(
                f'Repository "{repository!r}" not found. Available repositories: {list(repositories.keys())}.'
            )
        return repositories[repository]

    def _valid_table(self, source, table):
        if table not in sources[source]:
            raise ValueError(
                f'Table "{table!r}" not found. Available tables: {sources[source]}.'
            )
        return

    @staticmethod
    def _valid_dates(repository, date_start, date_end):
        date_start = datetime.strptime(date_start, "%d/%m/%Y").date()
        date_end = datetime.strptime(date_end, "%d/%m/%Y").date()
        date_min = repository["date_min"]
        if date_start > date_end:
            raise ValueError("End date can not be greater than start date.")
        if date_start < date_min:
            raise ValueError(
                f"Start date can not be less than {date_min.strftime('%d/%m/%Y')}."
            )
        return date_start, date_end

    def _valid_states(self, states):
        if isinstance(states, list):
            for state in states:
                if state not in self._states:
                    raise ValueError(f'State "{state!r}" not found.')
                return states
        elif isinstance(states, str):
            if states == "all":
                return self._states
            else:
                if states in self._states:
                    return [states]
                else:
                    raise ValueError(f'State "{states!r}" not found.')
        else:
            raise ValueError("State is not a string or list.")
        return
