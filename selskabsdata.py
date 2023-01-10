import argparse
import json
import os
import subprocess
import xml.etree.ElementTree as ET
from typing import TypedDict

import requests


parser = argparse.ArgumentParser()
# Assets AverageNumberOfEmployees CurrentAssets EmployeeBenefitsExpense Equity
# GrossProfitLoss IncreaseOfCapital LiabilitiesAndEquity OtherReceivables
# ProfitLoss ProposedDividendRecognisedInEquity ShorttermReceivables
# ShorttermReceivablesFromGroupEnterprises ShorttermTradeReceivables
parser.add_argument("-k", "--key", default="ProfitLoss")
parser.add_argument("cvr", type=int, nargs="+")


class CvrDokument(TypedDict):
    dokumentType: str  # "AARSRAPPORT",
    dokumentMimeType: str  # "application/xml",
    dokumentUrl: str  # "http://regnskaber.virk.dk/xxxxxxxx/xxxxx...xml"


class CvrRegnskabsperiode(TypedDict):
    slutDato: str  # "2015-12-31"
    startDato: str  # "2015-01-01"


class CvrRegnskab(TypedDict):
    regnskabsperiode: CvrRegnskabsperiode


class CvrSag(TypedDict):
    # "_index": "indberetninger-xxxxxxxx",
    # "_type": "_doc",
    # "_id": "urn:ofk:oid:xxxxxxxx",
    # "_score": 1,
    # "indlaesningsId": null,
    # "sagsNummer": "...-..-..-..",
    regnskab: CvrRegnskab
    # "sidstOpdateret": "2016-06-07THH:MM:SS.mmmZ",
    # "cvrNummer": ...,
    dokumenter: list[CvrDokument]
    # "regNummer": null,
    # "indlaesningsTidspunkt": "2018-03-29THH:MM:SS.mmmZ",
    # "offentliggoerelsesTidspunkt": "2016-06-07THH:MM:SS.mmmZ",
    # "omgoerelse": false,
    # "offentliggoerelsestype": "regnskab"


def do_search_documents(cvr: int) -> list[CvrSag]:
    elastic_query = {
        "query": {"bool": {"must": {"term": {"cvrNummer": cvr}}}},
        "size": 2999,
    }
    with requests.post(
        "http://distribution.virk.dk/offentliggoerelser/_search", json=elastic_query
    ) as response:
        return [d["_source"] for d in response.json()["hits"]["hits"]]


def search_documents(cvr: int) -> list[CvrSag]:
    filename = "documents%s.json" % cvr
    try:
        with open(filename) as fp:
            return json.load(fp)
    except FileNotFoundError:
        pass
    result = do_search_documents(cvr)
    with open(filename, "w") as fp:
        fp.write(json.dumps(result) + "\n")
    return result


def parse_context(t):
    entity_cvr: int | None = None
    period = None
    scenario = None
    for c in t:
        if c.tag == "{http://www.xbrl.org/2003/instance}entity":
            (i,) = c
            assert i.tag == "{http://www.xbrl.org/2003/instance}identifier", i.tag
            assert i.attrib["scheme"] == "http://www.dcca.dk/cvr", i.attrib["scheme"]
            entity_cvr = int(i.text.replace(" ", ""))
        elif c.tag == "{http://www.xbrl.org/2003/instance}period":
            period_xml = {i.tag: i.text for i in c}
            if period_xml.keys() == {"{http://www.xbrl.org/2003/instance}instant"}:
                period = period_xml["{http://www.xbrl.org/2003/instance}instant"]
            elif period_xml.keys() == {
                "{http://www.xbrl.org/2003/instance}startDate",
                "{http://www.xbrl.org/2003/instance}endDate",
            }:
                period = (
                    period_xml["{http://www.xbrl.org/2003/instance}startDate"],
                    period_xml["{http://www.xbrl.org/2003/instance}endDate"],
                )
            else:
                raise Exception(period_xml)
        elif c.tag == "{http://www.xbrl.org/2003/instance}scenario":
            scenario = c
        else:
            raise Exception(c.tag)
    if scenario is not None:
        return None
    assert entity_cvr is not None
    assert period is not None
    return {"cvr": entity_cvr, "period": period}


def get_xbrl(url: str):
    filename = os.path.basename(url)
    if not os.path.exists(filename):
        subprocess.check_call(("curl", "--compressed", "--output", filename, url))
    doc = ET.parse(filename)
    SKIP = [
        "{http://www.xbrl.org/2003/linkbase}schemaRef",
    ]
    ns_gsd = "{http://xbrl.dcca.dk/gsd}"
    ns_fsa = "{http://xbrl.dcca.dk/fsa}"
    ns_cmn = "{http://xbrl.dcca.dk/cmn}"
    ns_sob = "{http://xbrl.dcca.dk/sob}"
    ns_arr = "{http://xbrl.dcca.dk/arr}"
    ns_mrv = "{http://xbrl.dcca.dk/mrv}"
    contexts = {}
    units = {}
    kvp = []
    for t in doc.getroot():
        if t.tag in SKIP:
            continue

        if t.tag == "{http://www.xbrl.org/2003/instance}context":
            parsed_context = parse_context(t)
            if parsed_context is not None:
                contexts[t.attrib["id"]] = parsed_context
            continue

        if t.tag == "{http://www.xbrl.org/2003/instance}unit":
            (m,) = t
            assert m.tag == "{http://www.xbrl.org/2003/instance}measure", m.tag
            units[t.attrib["id"]] = m.text
            continue

        if t.tag.startswith(ns_gsd):
            kind = "gsd"
            k = t.tag.partition(ns_gsd)[2]
            v = t.text
        elif t.tag.startswith(ns_fsa):
            kind = "fsa"
            k = t.tag.partition(ns_fsa)[2]
            v = t.text
        elif t.tag.startswith(ns_cmn):
            kind = "cmn"
            k = t.tag.partition(ns_cmn)[2]
            v = t.text
        elif t.tag.startswith(ns_sob):
            kind = "sob"
            k = t.tag.partition(ns_sob)[2]
            v = t.text
        elif t.tag.startswith(ns_arr):
            kind = "arr"
            k = t.tag.partition(ns_arr)[2]
            v = t.text
        elif t.tag.startswith(ns_mrv):
            kind = "mrv"
            k = t.tag.partition(ns_mrv)[2]
            v = t.text
        else:
            raise Exception(t.tag)
        context_id = t.attrib["contextRef"]
        kvp.append((kind, k, context_id, v))
    return {
        "kvp": kvp,
        "context": contexts,
        "unit": units,
    }


def main() -> None:
    args = parser.parse_args()
    result = []
    for the_cvr in args.cvr:
        docs = search_documents(the_cvr)
        xbrls = []
        for doc in docs:
            period = (
                doc["regnskab"]["regnskabsperiode"]["startDato"],
                doc["regnskab"]["regnskabsperiode"]["slutDato"],
            )
            for dok in doc["dokumenter"]:
                if dok["dokumentMimeType"] != "application/xml":
                    continue
                url = dok["dokumentUrl"]
                xbrls.append((period, url))
        xbrls.sort(key=str)
        for period, url in xbrls:
            xbrl = get_xbrl(url)
            (full_period_context,) = [
                c
                for c in xbrl["context"]
                if xbrl["context"][c]["cvr"] == the_cvr
                and xbrl["context"][c]["period"] == period
            ]
            (end_context,) = [
                c
                for c in xbrl["context"]
                if xbrl["context"][c]["cvr"] == the_cvr
                and xbrl["context"][c]["period"] == period[1]
            ]
            names = set(
                value
                for kind, key, context_id, value in xbrl["kvp"]
                if kind == "gsd"
                and key == "NameOfReportingEntity"
                and context_id == full_period_context
            )
            assert len(names) == 1, names
            (name,) = names
            values = [
                value
                for kind, key, context_id, value in xbrl["kvp"]
                if key == args.key and context_id in (full_period_context, end_context)
            ]
            if values:
                assert len(set(values)) == 1, (period, name, values)
                value = values[0]
                result.append((period, the_cvr, name, value))
    periods = sorted(set(r[0][0][:4] for r in result))
    for the_cvr, the_name in {c: n for p, c, n, v in result}.items():
        print(the_cvr, the_name)
    row_str = [""] + [str(the_cvr) for the_cvr in args.cvr] + ["SUM"]
    print("".join(v.rjust(12) for v in row_str).rstrip())
    for year in periods:
        row = []
        s = 0
        for the_cvr in args.cvr:
            r = [v for p, c, n, v in result if p[0][:4] == year and c == the_cvr]
            row.append(r[0] if r else None)
            if r:
                s += int(r[0])
        row_str = [year] + ["" if v is None else str(v) for v in row] + [str(s)]
        print("".join(v.rjust(12) for v in row_str).rstrip())


if __name__ == "__main__":
    main()
