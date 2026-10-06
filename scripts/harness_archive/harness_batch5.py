import importlib.util, sys, types, os
for m in ("aiohttp","requests","googleapiclient","googleapiclient.discovery","google","google.oauth2","google.oauth2.service_account"):
    sys.modules.setdefault(m, types.ModuleType(m))
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    mod=importlib.util.module_from_spec(spec); sys.modules[name]=mod; spec.loader.exec_module(mod); return mod
sel=load("t10","top10_selector.py"); ob=load("ob","opportunity_builder.py")
def run(tag):
    ok=tot=0
    def t(n,c):
        nonlocal ok,tot; tot+=1; assert c,n; ok+=1; print("PASS",n)
    os.environ.pop("TFB_T10_DISCLOSE_LEGACY",None)
    row={"advisor_note":"INVEST — 6,286 SAR (951 sh) ...","funds_from":"Cash 6,286 SAR",
         "suggested_sar":6286,"suggested_shares":951,"exp_gain_12m_sar":742,
         "engine_exp_gain_12m_sar":1735,
         "ticket":{"funds_from":"Cash 6,286 SAR","suggested_sar":6286,
                   "suggested_shares":951,"exp_gain_12m_sar":742,
                   "engine_exp_gain_12m_sar":1735,"entry_zone":"6.41–6.68 SAR",
                   "stop_sar":6.10,"advisor_note":"INVEST — 6,286 SAR ..."}}
    sel._t10_redact_withheld(row,"FAST-TRACK (day 1)")
    tk=row["ticket"]
    t("R1 nested funds redacted", tk["funds_from"]=="\u2014" and row["funds_from"]=="\u2014")
    t("R2 nested sizing None", tk["suggested_sar"] is None and tk["suggested_shares"] is None
      and tk["exp_gain_12m_sar"] is None and tk["engine_exp_gain_12m_sar"] is None
      and row["engine_exp_gain_12m_sar"] is None)
    t("R3 levels survive (BC-4)", tk["entry_zone"]=="6.41–6.68 SAR" and tk["stop_sar"]==6.10)
    t("R4 nested note research-only", "RESEARCH_ONLY" in tk["advisor_note"] and "6,286" not in tk["advisor_note"])
    row2={"funds_from":"Cash 1 SAR","ticket":{"funds_from":"Cash 1 SAR","suggested_sar":1}}
    sel._t10_redact_withheld(row2,"ACTIVE (day 4)")
    t("R5 ACTIVE untouched", row2["ticket"]["funds_from"]=="Cash 1 SAR" and row2["ticket"]["suggested_sar"]==1)
    os.environ["TFB_T10_DISCLOSE_LEGACY"]="1"
    row3={"funds_from":"Cash 2 SAR","ticket":{"funds_from":"Cash 2 SAR"}}
    sel._t10_redact_withheld(row3,"GRACE (2/3 missed)")
    t("R6 legacy escape verbatim", row3["ticket"]["funds_from"]=="Cash 2 SAR")
    os.environ.pop("TFB_T10_DISCLOSE_LEGACY",None)
    # builder gates
    os.environ.pop("TFB_OPP_FUNDING_SETTLED_ONLY",None)
    lbl,c,p=ob._funds_from(1000.0, 600.0, 1000.0)
    t("B1 default splits cash+proceeds", "Cash" in lbl and "proceeds" in lbl and c==0.0 and p==600.0)
    os.environ["TFB_OPP_FUNDING_SETTLED_ONLY"]="1"
    lbl,c,p=ob._funds_from(1000.0, 600.0, 1000.0)
    t("B2 settled-only zeroes proceeds", "proceeds" not in lbl and c==0.0 and p==0.0 and "Cash" in lbl)
    lbl,c,p=ob._funds_from(1000.0, 0.0, 500.0)
    t("B2b settled-only unfunded", lbl.startswith("Unfunded"))
    os.environ.pop("TFB_OPP_FUNDING_SETTLED_ONLY",None)
    t("B3 split store keys", set(ob._LAST_DEPLOYABLE_SPLIT)== {"current","proforma"})
    os.environ.pop("TFB_OPP_SIZE_AT_ENTRY_HIGH",None)
    t("B4 gates default off", ob._funding_settled_only() is False and ob._size_at_entry_high() is False)
    os.environ["TFB_OPP_SIZE_AT_ENTRY_HIGH"]="1"
    t("B4b gate parses", ob._size_at_entry_high() is True)
    os.environ.pop("TFB_OPP_SIZE_AT_ENTRY_HIGH",None)
    print(f"[{tag}] {ok}/{tot} PASS")
for i in (1,2,3): run(f"run{i}")
print("BATCH5 HARNESS 3/3 CLEAN")
