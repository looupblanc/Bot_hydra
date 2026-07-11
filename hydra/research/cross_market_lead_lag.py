from __future__ import annotations

import json, math, subprocess
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd
from hydra.data.contract_mapping import load_roll_map
from hydra.mission.calibration_retest_execution import DEFAULT_HISTORICAL_REPORT,_build_past_only_feature_frame,_file_sha256,_future_target,_load_governed_development_frame,_load_markdown_json,_stable_hash,_strict_json_value,_verify_development_manifest

MAP_TYPE="EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2"
MARKETS=("NQ","ES","RTY","YM"); FOLDS={"2023_h2":("2023-07-01","2024-01-01"),"2024_q1":("2024-01-01","2024-04-01"),"2024_q2":("2024-04-01","2024-07-01"),"2024_q3":("2024-07-01","2024-10-01")}
COST={"ES":9.,"NQ":9.,"RTY":9.,"YM":9.}; POINT={"ES":50.,"NQ":20.,"RTY":50.,"YM":5.}
class CrossMarketLeadLagError(RuntimeError): pass

def run_cross_market_lead_lag_pilot(output_dir: str|Path, *, engineering_task_path: str|Path, engineering_task_sha256: str, repaired_map_path: str|Path, repaired_map_sha256: str, repaired_roll_map_hash: str, code_commit: str, record_data_access: bool=True)->dict[str,Any]:
    task,map_path=Path(engineering_task_path),Path(repaired_map_path)
    _verify(task,engineering_task_sha256); _verify(map_path,repaired_map_sha256); roll=load_roll_map(map_path)
    if roll.map_type!=MAP_TYPE or roll.roll_map_hash()!=repaired_roll_map_hash: raise CrossMarketLeadLagError("map mismatch")
    if len(code_commit)==40 and subprocess.check_output(["git","rev-parse","HEAD"],text=True).strip()!=code_commit: raise CrossMarketLeadLagError("commit mismatch")
    historical=_load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT)); source=Path('/root/hydra-bot/reports/mission_experiments/calibration_affected_atom_retest_v3_design_v1/calibration_affected_atom_retest_v3_preregistration.json'); _verify(source,'d3e6ab3fe77ccb759902bb2241fef8e6203e583259eb25648d739fa751b15e26'); _verify_development_manifest((json.loads(source.read_text()).get('source') or {}).get('development_data_manifest') or {})
    pre={'schema':'cross_market_lead_lag_preregistration_v1','atom_id':'atom_cross_market_nq_lead_lag_20260711_v1','markets':list(MARKETS),'horizon':30,'leader_lag_bars':1,'task_sha256':engineering_task_sha256,'map_sha256':repaired_map_sha256,'roll_map_hash':repaired_roll_map_hash,'code_commit':code_commit,'q4':False}; pre['preregistration_hash']=_stable_hash(pre)
    out=Path(output_dir); out.mkdir(parents=True,exist_ok=True); pre_path=out/'cross_market_lead_lag_preregistration.json'; _write(pre_path,json.dumps(pre,indent=2,sort_keys=True)+'\n')
    access=None
    if record_data_access:
        from hydra.mission.calibration_retest_execution import _record_data_access_once
        access=_record_data_access_once('2023-01-01:2024-10-01',[pre['atom_id']],'cross-market lead lag development pilot; Q4 excluded')
    raw,prov=_load_governed_development_frame(historical,[{'target_markets':list(MARKETS)}],contract_map_path=map_path,required_contract_map_type=MAP_TYPE); f=_build_past_only_feature_frame(raw); f['target']=_future_target(f,30,defensive=False); rows=_evaluate(f)
    gates={'temporal_transfer':all(rows[x]['net']>0 for x in ('2024_q1','2024_q2','2024_q3')),'transfer_markets':all(rows[x]['events']>=30 for x in ('2024_q1','2024_q2','2024_q3')),'finite':all(math.isfinite(rows[x]['net']) for x in rows)}
    payload={'schema':'cross_market_lead_lag_pilot_v1','scientific_conclusion':'CROSS_MARKET_LEAD_LAG_SURVIVES_DEVELOPMENT' if all(gates.values()) else 'CROSS_MARKET_LEAD_LAG_FAILS_OR_INSUFFICIENT','mechanism_status':'DEVELOPMENT_SURVIVOR' if all(gates.values()) else 'NOT_VALIDATED','gates':gates,'fold_results':rows,'preregistration_hash':pre['preregistration_hash'],'preregistration_path':str(pre_path),'data_provenance':prov,'data_access_record':access,'validated_mechanisms':0,'validated_strategies':0,'governance':{'q4':False,'paid':False,'live':False}}
    payload=_strict_json_value(payload); payload['result_hash']=_stable_hash(payload); rp=out/'cross_market_lead_lag_result.json'; report=out/'cross_market_lead_lag_report.md'; _write(rp,json.dumps(payload,indent=2,sort_keys=True)+'\n'); _write(report,f"# Cross-market lead/lag\n\n- Conclusion: `{payload['scientific_conclusion']}`\n- Gates: `{payload['gates']}`\n"); return {**payload,'artifacts':{'result_json_path':str(rp),'report_path':str(report)},'report_path':str(report)}

def _evaluate(f:pd.DataFrame)->dict[str,Any]:
    out={}
    for fold,(start,end) in FOLDS.items():
        x=f[(f.trading_session_id>=start)&(f.trading_session_id<end)].copy()
        leader=x[x.symbol=='NQ'][['timestamp','active_contract','contiguous_segment_id','close']].copy()
        leader['leader_return']=leader.groupby(['active_contract','contiguous_segment_id'],sort=False).close.pct_change(5).shift(1)
        leader=leader[['timestamp','leader_return']].drop_duplicates('timestamp')
        lag=x[x.symbol!='NQ'].merge(leader,on='timestamp',how='inner').dropna(subset=['leader_return','target'])
        lag=lag[lag.leader_return.abs()>0.00015]; lag['pnl']=np.sign(lag.leader_return)*lag.target*lag.close*lag.symbol.map(POINT)-lag.symbol.map(COST); out[fold]={'net':float(lag.pnl.sum()),'events':int(len(lag)),'markets':{str(m):int((lag.symbol==m).sum()) for m in MARKETS if m!='NQ'}}
    return out

def _verify(path:Path,sha:str)->None:
    if not path.is_file() or _file_sha256(path)!=sha: raise CrossMarketLeadLagError('frozen artifact mismatch')
def _write(path:Path,text:str)->None:
    if path.exists() and path.read_text()!=text: raise CrossMarketLeadLagError('immutable artifact conflict')
    if not path.exists(): path.write_text(text)
