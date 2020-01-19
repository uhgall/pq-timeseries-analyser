# generic .parquet analysis tool for time series data
# each file must have a column called "timestamp"
# Ulrich Gall, 2020-01-19

import time
import re
import sys
import os
import collections
import math
from os import listdir
from os.path import isfile, join
from string import Template

import pprint
pp = pprint.PrettyPrinter(indent=2)

import json
import pyjade
import numpy as np
import pandas as pd
import colorlover as cl

if len(sys.argv) == 1:
  path = "pqfiles"
  print("Specify path to parquet files, please. Since you did not, we will use %s" % path)
else:
  path = sys.argv[1]

files={}
dfs ={}
pq_attrs = []

hidden_min_change_percent = 3 # ignore variables that change less than x% ofthe time
hidden_varnames =  ["tx_msg_num","rx_time","rx_delay","msg_count","in_percent","out_percent","magswitch_analog_max","command_value"]
hidden_varprefixes = ["GpsTime"]

d = { "totalconstwaste": 0, 
      "totalfilesize": 0, 
      "statecriterion": 6 # if we observe more then this no of distinct values, we call it a variable
    }

for f in listdir(path):
  if (not f.endswith(".parquet")):
    continue
  pf = join(path, f)
  varprefix = re.sub('\.parquet$', '', f)

  df = pd.read_parquet(pf, engine='pyarrow')
  #df = df.truncate(after=10000) # for testing

  filesize = os.stat(pf).st_size
  rowcount = len(df.index)
  fieldcount = df.size
  fieldsize = filesize / fieldcount

  tsdiff = df["timestamp"].diff()
  tsdiffmean = tsdiff.mean()
  if math.isnan(tsdiffmean):
    tsdiffmean = 0
 
  files[varprefix] = {"rowcount": rowcount, 
                      "filesize":filesize,
                      "fieldcount":fieldcount,
                      "fieldsize":round(fieldsize),
#                      "tsdiffcount": tsdiff.nunique(),
                      "update_interval": round(tsdiffmean)}
  dfs[varprefix] = df

  attrcount = 0
  constcount = 0
  nu = df.nunique()
  for varname, count in nu.items():
    if varname == "timestamp":
      continue
    if varname == "timediff":
      continue
    dtype = df[varname].dtype
    v = {"varprefix":varprefix,"varname":varname,"dtype":dtype,
        "rowcount": rowcount,"update_interval": tsdiffmean}
    attrcount += 1
    if count == 1:
      v.update({"type":"constant","value":df[varname][0] })
      constcount += 1
    elif count <= d["statecriterion"]:
      counts = df[varname].value_counts()
      diffcounts = (df[varname]*1).diff().value_counts() # *1 converts boolean to 0 and 1
      if dtype.name == "bool":
        changecount = (diffcounts.get(1.0) or 0) + (diffcounts.get(-1.0) or 0)
        v.update({"type":"boolean","true": counts[True], "false": counts[False], "changes": changecount})
      else:
        samecount = diffcounts.pop(0.0)
        if (samecount==None):
          diffcount = rowcount
        else:
          diffcount = diffcounts.values.sum()  
        v.update({"type":"state","seencount": count,"samecount":samecount,"diffcount": diffcount,
          "allcounts":counts.to_dict(),"allchanges": diffcounts.to_dict()})
     
    else:
      dfv = df[varname]
      vardiff = dfv.diff()
      varchanged = vardiff.between(0,0).value_counts()[False] # [True] would sometimes throw an error...
      varchangedpercent = round(100*varchanged/rowcount)

      varshown = True
      if varchangedpercent < hidden_min_change_percent:
        varshown="No, Differs less than %d percent of the time" % hidden_min_change_percent
      if varname in hidden_varnames:
        varshown="No, Variable on hide list"
      if varprefix in hidden_varprefixes:
        varshown="No, File on hide list" 

      v.update({"type":"scalar","seencount": count,"varchangedpercent":varchangedpercent,
        "min":dfv.min(),"max":dfv.max(),"varshown":varshown})
    pq_attrs.append(v)

  constwaste =  round(constcount/attrcount * filesize) # timestamp always needed
  files[varprefix]["constwaste"] = constwaste
  d["totalconstwaste"] += constwaste
  d["totalfilesize"] += filesize

d["constwastepercent"] = round(100 * d["totalconstwaste"]  /  d["totalfilesize"] )
d["totalconstwastemb"] = round(d["totalconstwaste"] /1000000)
d["totalfilesizemb"] =   round(d["totalfilesize"] /1000000)

d["files"] =     pd.DataFrame(data=files, dtype=np.int64).transpose().to_html()

def make_html(v,others):
  d[v]= (attr_df[attr_df.type == v][shared_fields+others]).to_html()

colors =  cl.scales['12']["qual"]["Paired"]  + cl.scales['12']["qual"]["Set3"]


def make_traces(divname,rows):
  traces = []
  layout = {'hovermode': 'closest',    
    "xaxis":{"showline":True,"showticklabels":True,"ticks":"outside","title":{"text":"Time in seconds"}}
  }
  i=1
  for row in rows.itertuples():
    varname = row.varname
    varprefix = row.varprefix
    if row.varshown != True:
      continue
    df = dfs[varprefix][["timestamp",varname]]
    yvalues = []
    xvalues = []
    prevy = None
    prevx = None
    add_prev_if_y_diff = False
    for datarow in df.itertuples():
      x = datarow[1]
      y = datarow[2]
      if y!= prevy:
        if add_prev_if_y_diff:
          xvalues.append(prevx)
          yvalues.append(prevy)
        xvalues.append(x)
        yvalues.append(y)
        add_prev_if_y_diff = False
      else:
        add_prev_if_y_diff = True
      prevx=x
      prevy=y

    # update attr_df
    print("%s reduced from %d to %d" % (varname,len(df),len(xvalues)))
    trace = {
      "x":[x/1000 for x in xvalues],
      "y":yvalues,
      "type": "scatter", 
      "mode": "lines",
      "hovertemplate":"T+%{x:.3f} sec<br>"+varname+" = %{y:.2f}<br><extra></extra>",
      "name": varname+"("+varprefix+")"

    }
    if i>3:
      trace.update({"visible": "legendonly"})
    yaxisname = "yaxis"
    yaxisdict = {
  #    "range":[row.min,row.max],
      # "title":row.varname,
      "showline":True,
      "showgrid":False,
      "showticklabels":False
    } 
    
    if i > 1:
      trace.update({"yaxis":"y%i"%i})
      yaxisname = "yaxis%i"%i
      yaxisdict.update({"overlaying": 'y'})
    layout.update({yaxisname: yaxisdict})

    traces.append(trace)
    i += 1
    # if i>2:
    #   break
  return("Plotly.newPlot('%s',%s,%s)" % (divname,json.dumps(traces),json.dumps(layout)));

def booly(b,i):
  if b:
    return i
  else:
    return None

def booltext(varname,y):
  return("%s became %s" % (varname,y))

def make_boolean_traces(divname,rows):
  traces = []
  layout = {'hovermode': 'closest',  
    "yaxis":{"showline":False,"showticklabels":False,"zeroline":False},
    "xaxis":{"showline":True,"showticklabels":True,"ticks":"outside","title":{"text":"Time in seconds"}}
  }
  i=0
  for row in rows.itertuples():
    pos = -i-1
    varname = row.varname
    varprefix = row.varprefix
    df = dfs[varprefix][["timestamp",varname]]   
    truevalues = []
    falsevalues = []
    xvalues = []
    textvalues = []
    unchanged_since=0
    add_prev_if_y_diff = False
    fresh=True
    rowindex=0
    for datarow in df.itertuples():
      last_row = rowindex==len(df)
      rowindex+=1
      if fresh:
        prevx=datarow[1]
        prevy=datarow[2]
        xvalues.append(prevx)
        textvalues.append("<b>%s</b> started out as <b>%s</b>"%(varname,prevy))
        if prevy:
          truevalues.append(pos)
          falsevalues.append(None)
        else:
          falsevalues.append(pos+0.3)
          truevalues.append(None)

        fresh=False
        continue
      x = datarow[1]
      y = datarow[2]
      if y!= prevy or last_row:
        if add_prev_if_y_diff:
          xvalues.append(prevx)
          textvalues.append("<b>%s</b> was <b>%s</b><br>Until after T+%.3fs<br>For at least %.3fs"%(varname,prevy,prevx/1000,(prevx-unchanged_since)/1000))
          if prevy:
            truevalues.append(pos)
            falsevalues.append(None)
          else:
            falsevalues.append(pos+0.3)
            truevalues.append(None)
        xvalues.append(x)
        textvalues.append("<b>%s</b> became <b>%s</b><br>before T+%.3fs"%(varname,y,x/1000))
        if y:
          truevalues.append(pos)
          falsevalues.append(None)
        else:
          falsevalues.append(pos+0.3)
          truevalues.append(None)
        
        add_prev_if_y_diff = False
        unchanged_since=x
      else:
        add_prev_if_y_diff = True
      prevx=x
      prevy=y

    # update attr_df
    print("%s reduced from %d to %d" % (varname,len(df),len(xvalues)))
    trace = {
      "x":[x/1000 for x in xvalues],
      "y":truevalues,
      "text": textvalues,
      "hovertemplate":"%{text}<extra></extra>",
      "type": "scatter", 
      "mode": "lines+markers",      
      "line": {"width":2,"color":colors[i]},
      "name": (varname+"("+varprefix+")")
    }
    traces.append(trace)
    trace = {
      "x":[x/1000 for x in xvalues],
      "y":falsevalues,
      "text": textvalues,
      "hovertemplate":"%{text}<extra></extra>",
      "type": "scatter", 
      "mode": "lines+markers",
      "line": {"dash":"dot", "width":2,"color":colors[i]},
      "showlegend":False,
      "marker": {"symbol": "x"},
      "name": varname+"("+varprefix+")-False"
    }
    traces.append(trace)
    i += 1
  return("Plotly.newPlot('%s',%s,%s)" % (divname,json.dumps(traces),json.dumps(layout)));

attr_df = pd.DataFrame(pq_attrs)

rows = attr_df[attr_df.type =="scalar"]
d["scalarplot"] = make_traces("scalarplot",rows)

rows = attr_df[attr_df.type =="boolean"]
d["booleanplot"] = make_boolean_traces("booleanplot",rows)
d["boolean_plot_height_px"] = len(rows)*34

shared_fields = ["varprefix","varname","dtype","rowcount","update_interval"]

make_html("constant",["value"])
make_html("boolean",["true", "false", "changes"])
make_html("state",["seencount","samecount","diffcount","allcounts","allchanges"])
make_html("scalar",["seencount","varchangedpercent","min","max","varshown"])

templ = '''

<head>
<script src="plotly-latest.min.js"></script>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>

<body>
<h1>Time Series Graph</h1>
Not all variables are shown. Scroll down for more details on which ones and why. 
Click on traces in legend to show/hide.

<div id="scalarplot" style="width:1200px;height:800px;"></div>
<p>
For booleans that actually change, we came up with the following way to visualize them:
</p>
<div id="booleanplot" style="width:1200px;height:$boolean_plot_height_px;"></div>

<h2>Files</h2>
<p> Total file size was $totalfilesizemb MB. Of this, approximately $totalconstwastemb MB was used for constants. That's $constwastepercent %.</p>
<p> $files </p>

<h2>Constants</h2>
<p> These always have the same value. Not including them in graphs. </p>
<p> $constant </p>

<h2>Boolean Variables</h2>
All graphed, see above.
<p> $boolean </p>

<h2>State Variables</h2>
<p> Variables with $statecriterion or fewer observed unique values will be visualized in the graphs in some reasonable way - TODO. </p>
<p> $state </p>

<h2>Scalar Variables</h2>
<p> $scalar </p>

<script>
$scalarplot
$booleanplot

</script>

</body>

'''

html = Template(templ).substitute(d);

with open('index.html', 'w') as file:
    file.write(html)

