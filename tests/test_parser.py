import os
import sys
import textwrap

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from edgar.parser import parse_file_list

HTML = textwrap.dedent(
    """
<table class="tableFile">
<tr><th>Seq</th><th>Description</th><th>Document</th><th>Type</th><th>Size</th></tr>
<tr><td>1</td><td>Main Document</td><td><a href="/x.htm">x.htm</a></td><td>10-K</td><td>100 KB</td></tr>
<tr><td>2</td><td>Exhibit</td><td><a href="/y.htm">y.htm</a></td><td>EX-99</td><td>50 KB</td></tr>
</table>
"""
)


def test_parse_file_list():
    files = parse_file_list(HTML)
    assert len(files) == 2
    assert files[0]["document"] == "x.htm"
    assert files[1]["type"] == "EX-99"
