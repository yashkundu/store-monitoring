import pandas as pd

class ReportDb:
    def __init__(self):
        # not in dict status - wrong report_id
        # 1 - processing
        # 2 - processed
        self.report_id = None
        self.status = None
        

    def startReportProcessing(self, report_id: str):
        self.report_id = report_id
        self.status = 1

    def endReportProcessing(self, report_id: str):
        self.status = 2


rdb = ReportDb()