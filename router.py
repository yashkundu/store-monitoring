from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import StreamingResponse
from generate_report import generate_report
import secrets
from reports import rdb
import pandas as pd

router = APIRouter()

@router.get('/trigger_report')
def trigger_report(background_tasks: BackgroundTasks):
    report_id = secrets.token_hex(10)
    rdb.startReportProcessing(report_id)
    background_tasks.add_task(generate_report, report_id=report_id)
    return {'report_id': report_id}


@router.get('/get_report')
def get_report(report_id: str):
    if report_id != rdb.report_id:
        raise HTTPException(status_code=404, detail='no report with that id exists')
    if rdb.status==1:
        return {'msg': 'Running'}
    else:
        df = pd.read_csv('data.csv')
        return StreamingResponse(iter([df.to_csv(index=False)]), media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=data.csv"})