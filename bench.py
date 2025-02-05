from typing import Tuple
import requests as r

import lxml.html

def details_from_url(url: str) -> Tuple[str, str, str]:
    data = r.get(url)
    body = data.content
    tree = lxml.html.fromstring(body)

    name = tree.xpath("string(/html/body/div/table/tr/td/table/tr[3]/td/b)").strip()
    regnum = tree.xpath("string(/html/body/div/table/tr/td/table/tr[4]/td)").replace("Reg No:", "").strip()
    dept = tree.xpath("string(/html/body/div/table/tr/td/table/tr[5]/td)").strip()

    return (name, regnum, dept)
