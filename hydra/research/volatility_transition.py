from __future__ import annotations
import json, math, subprocess
from pathlib import Path
from typing import Any
import numpy as np
import pandas as pd
from hydra.data.contract_mapping import load_roll_map
from hydra.mission.calibration_retest_execution import DEFAULT_HISTORICAL_REPORT,_build_past_only_feature_frame,_file_sha256,_future_target,_load_governed_development_frame,_load_markdown_json,_stable_hash,_strict_json_value,_verify_development_manifest

MAP_TYPE="EXPLICIT_DATABENTO_CONTINUOUS_SYMBOLOGY_DATE_AWARE_DEFINITIONS_V2"; MARKETS=("ES","NQ","RTY","YM","GC","CL"); COST=9.0; POINT={"ES":50.,"NQ":20.,"RTY":50.,"YM":5.,"GC":100.,"CL":1000.}
FOLDS={"2023_h2":("2023-07-01","2024-01-01"),"2024_q1":("2024-01-01","2024-04-01"),"2024_q2":("2024-04-01","2024-07-01"),"2024_q3":("2024-07-01","2024-10-01")}
class VolatilityTransitionError(RuntimeError): pass

def run_volatility_transition_pilot(output_dir: str|Path, *, engineering_task_path: str|Path, engineering_task_sha256: str, repaired_map_path: str|Path, repaired_map_sha256: str, repaired_roll_map_hash: str, code_commit: str, record_data_access: bool=True)->dict[str,Any]:
    task,mp=Path(engineering_task_path),Path(repaired_map_path); _verify(task,engineering_task_sha256); _verify(mp,repaired_map_sha256); roll=load_roll_map(mp)
    if roll.map_type!=MAP_TYPE or roll.roll_map_hash()!=repaired_roll_map_hash: raise VolatilityTransitionError('map mismatch')
    if len(code_commit)==40 and subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()!=code_commit: raise VolatilityTransitionError('commit mismatch')
    hist=_load_markdown_json(Path(DEFAULT_HISTORICAL_REPORT)); src=Path('/root/hydra-bot/reports/mission_experiments/calibration_affected_atom_retest_v3_design_v1/calibration_affected_atom_retest_v3_preregistration.json'); _verify(src,'d3e6ab3fe77ccb759902bb2241fef8e6203e583259eb25648d739fa751b15e26'); _verify_development_manifest((json.loads(src.read_text()).get('source') or {}).get('development_data_manifest') or {})
    pre={'schema':'volatility_transition_preregistration_v1','atom_id':'atom_volatility_transition_20260711_v1','markets':list(MARKETS),'horizon':30,'upper_threshold':1.2,'lower_threshold':0.8,'task_sha256':engineering_task_sha256,'map_sha256':repaired_map_sha256,'roll_map_hash':repaired_roll_map_hash,'code_commit':code_commit,'q4':False}; pre['preregistration_hash']=_stable_hash(pre); out=Path(output_dir);out.mkdir(parents=True,exist_ok=True);pp=out/'volatility_transition_preregistration.json';_write(pp,json.dumps(pre,indent=2,sort_keys=True)+'\n')
    access=None
    if record_data_access:
        from hydra.mission.calibration_retest_execution import _record_data_access_once
        access=_record_data_access_once('2023-01-01:2024-10-01',[pre['atom_id']],'volatility transition development pilot; Q4 excluded')
    raw,prov=_load_governed_development_frame(hist,[{'target_markets':list(MARKETS)}],contract_map_path=mp,required_contract_map_type=MAP_TYPE);f=_build_past_only_feature_frame(raw);f['target']=_future_target(f,30,defensive=False);rows=_evaluate(f);g={'temporal_transfer':all(rows[x]['net']>0 for x in ('2024_q1','2024_q2','2024_q3')),'events':all(rows[x]['events']>=30 for x in ('2024_q1','2024_q2','2024_q3')),'finite':all(math.isfinite(rows[x]['net']) for x in rows)}
    p={'schema':'volatility_transition_pilot_v1','scientific_conclusion':'VOLATILITY_TRANSITION_SURVIVES_DEVELOPMENT' if all(g.values()) else 'VOLATILITY_TRANSITION_FAILS_OR_INSUFFICIENT','mechanism_status':'DEVELOPMENT_SURVIVOR' if all(g.values()) else 'NOT_VALIDATED','gates':g,'fold_results':rows,'preregistration_hash':pre['preregistration_hash'],'preregistration_path':str(pp),'data_provenance':prov,'data_access_record':access,'validated_mechanisms':0,'validated_strategies':0,'governance':{'q4':False,'paid':False,'live':False}};p=_strict_json_value(p);p['result_hash']=_stable_hash(p);rp=out/'volatility_transition_result.json';report=out/'volatility_transition_report.md';_write(rp,json.dumps(p,indent=2,sort_keys=True)+'\n');_write(report,f"# Volatility Transition\n\n- Conclusion: `{p['scientific_conclusion']}`\n- Gates: `{p['gates']}`\n");return {**p,'artifacts':{'result_json_path':str(rp),'report_path':str(report)},'report_path':str(report)}

def _evaluate(f:pd.DataFrame)->dict[str,Any]:
    out={}
    for fold,(start,end) in FOLDS.items():
        x=f[(f.trading_session_id>=start)&(f.trading_session_id<end)].copy();ratio=x['rv_short_long_ratio']; prev=ratio.groupby([x.symbol,x.active_contract,x.contiguous_segment_id]).shift(1);cross=(ratio>1.2)&(prev<=1.2);x=x[cross].dropna(subset=['target','past_return_60']);x['pnl']=np.sign(x['past_return_60'])*x.target*x.close*x.symbol.map(POINT)-COST;out[fold]={'net':float(x.pnl.sum()),'events':int(len(x)),'markets':{str(m):int((x.symbol==m).sum()) for m in MARKETS}}
    return out
def _verify(p:Path,s:str)->None:
    if not p.is_file() or _file_sha256(p)!=s:raise VolatilityTransitionError('frozen artifact mismatch')
def _write(p:Path,t:str)->None:
    if p.exists() and p.read_text()!=t:raise VolatilityTransitionError('immutable conflict')
    if not p.exists():p.write_text(t)
