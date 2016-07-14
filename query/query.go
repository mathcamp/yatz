package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/codeskyblue/go-sh"
	"github.com/mathcamp/prototypes/server/bqutil"
	"os"
	"strings"
	"text/template"
	"time"
)

const (
	SelectTypeUnique = "unique"
	SelectTypeCount  = "count"
)

type queryParams struct {
	exp        string
	events     []string
	start      time.Time
	end        time.Time
	selectType string
	print      bool
}

type templateParams struct {
	Experiment string
	Tables     string
	Events     string
	StartTime  string
	EndTime    string
}

func main() {
	qp := parseInput()
	params := qp.templateParams()
	var sel string
	switch qp.selectType {
	case SelectTypeCount:
		sel = countSelects
	case SelectTypeUnique:
		sel = uniqueSelects
	default:
		sel = bothSelects
	}
	var t *template.Template
	if qp.exp != "" {
		t = template.Must(template.Must(template.Must(template.Must(template.Must(template.New("query").
			Parse(perUserQuery)).Parse(eventsByUser)).Parse(weights)).Parse(events)).Parse(sel))
	} else {
		fmt.Println("Running query for only events")
		t = template.Must(template.Must(template.Must(template.New("query").
			Parse(onlyEvents)).Parse(eventsByUser)).Parse(events))
	}

	var buf bytes.Buffer
	t.Execute(&buf, params)

	fmt.Printf("Running query from %s to %s\n", qp.start.UTC().String(), qp.end.UTC().String())
	sess := sh.Command("bq", "query", "--max_rows", "5000", buf.String())
	sess.ShowCMD = qp.print
	sess.Run()
}

func (q *queryParams) templateParams() templateParams {
	tParams := templateParams{}
	tParams.Experiment = q.exp
	timeFormatstr := "2006-01-02 15:04:05"
	tParams.StartTime = q.start.UTC().Format(timeFormatstr)
	tParams.EndTime = q.end.UTC().Format(timeFormatstr)

	if len(q.events) > 0 {
		temp := make([]string, len(q.events))
		for idx, event := range q.events {
			temp[idx] = fmt.Sprintf("event=\"%s\"", event)
		}
		tParams.Events = strings.Join(temp, " OR ")
	}
	tParams.Tables = bqutil.GetTableNames("appengine_logs.appengine_googleapis_com_request_log_", q.start, q.end)
	return tParams
}

func parseInput() queryParams {
	start := flag.String("start", "", "Start time  (2015-04-11:01:30 PDT), default is end - 24 hours")
	end := flag.String("end", "", "End time (2015-04-12:17:45 UTC), default is now")
	events := flag.String("events", "", "Comma separated list of events you want to see. By default gives all of them")
	exp := flag.String("exp", "", "The experiment you want to dice by. If empty it will give info on just events")
	sel := flag.String("sel", "", "The select type, can be unique OR count")
	print := flag.Bool("print", false, "if this flag is set it will print the query")

	flag.Parse()

	qp := queryParams{}

	if exp != nil {
		qp.exp = *exp
	}

	if end == nil || *end == "" {
		qp.end = time.Now()
	} else {
		var err error
		qp.end, err = time.Parse("2006-01-02:15:04 MST", *end)
		if err != nil {
			fmt.Printf("Invalid end time: %s\n", err.Error())
			os.Exit(1)
		}
	}

	if start == nil || *start == "" {
		qp.start = qp.end.Add(-24 * time.Hour)
	} else {
		var err error
		qp.start, err = time.Parse("2006-01-02:15:04 MST", *start)
		if err != nil {
			fmt.Printf("Invalid start time: %s", err.Error())
			os.Exit(1)
		}
	}

	if qp.start.After(qp.end) {
		fmt.Printf("Start time: %s can't be after end time: %s\n", qp.start.String(), qp.end.String())
		os.Exit(1)
	}

	if events == nil || *events == "" {
		qp.events = []string{}
	} else {
		qp.events = strings.Split(*events, ",")
	}

	qp.print = *print
	qp.selectType = *sel
	return qp
}

const events = `
    {{define "events"}}
    SELECT protoPayload.line.time as time,
    protoPayload.line.logMessage as msg,
     REGEXP_EXTRACT(protoPayload.line.logMessage, r"^YATZ_EVENT\|(.+)\|.+\|.+\|.+\|.+") as userid,
     REGEXP_EXTRACT(protoPayload.line.logMessage, r"^YATZ_EVENT\|.+\|(.+)\|.+\|.+\|.+") as event,
     REGEXP_EXTRACT(protoPayload.line.logMessage, r"^YATZ_EVENT\|.+\|.+\|(.+)\|.+\|.+") as id,
     FLOAT(REGEXP_EXTRACT(protoPayload.line.logMessage, r"^YATZ_EVENT\|.+\|.+\|.+\|(.+)\|.+")) as cnt,
     REGEXP_EXTRACT(protoPayload.line.logMessage, r"^YATZ_EVENT\|.+\|.+\|.+\|.+\|(.+)") as variant,
    protoPayload.requestId as requestId,
    FROM {{$.Tables}}
    WHERE protoPayload.line.logMessage contains "YATZ_EVENT"
      and protoPayload.line.time > TIMESTAMP("{{.StartTime}}")
      and protoPayload.line.time < TIMESTAMP("{{.EndTime}}")
    {{if gt (len .Events) 0}}
    HAVING {{.Events}}
    {{end}}
    ORDER BY time asc
    {{end}}
`

const eventsByUser = `
  {{define "eventsByUser"}}
  SELECT event, userid,
  REGEXP_EXTRACT(variant, r"{{.Experiment}}:([a-zA-Z0-9-_]+)") as variant,
  ROUND(SUM(cnt), 2) as sum,
  COUNT(*) as num,
  COUNT(DISTINCT id) as uniques
  FROM
    (
    {{template "events" .}}
    )
  GROUP BY event, variant, userid
  {{end}}
`

const onlyEvents = `
	SELECT event, count(*) as users, SUM(num) as total_events,
	SUM(uniques) as unique_events, ROUND(AVG(uniques),2) as avg_uniques, ROUND(NTH(50, QUANTILES(uniques, 100)),2) as median_uniques,
	SUM(sum) as sum_of_counts, ROUND(AVG(sum), 2) as avg_count, ROUND(NTH(50, QUANTILES(sum, 100)),2) as median_value
	FROM(
	{{template "eventsByUser" .}})
	GROUP BY event
	ORDER BY users desc
`

const weights = `
  {{define "weights"}}
  SELECT time, exp, type,
  REGEXP_EXTRACT(variantWithWt, r"(.+)@[0-9/]+") as variant,
  ROUND(INTEGER(REGEXP_EXTRACT(variantWithWt, r".+@([0-9]+)/[0-9]+"))/INTEGER(REGEXP_EXTRACT(variantWithWt, r".+@[0-9]+/([0-9]+)")), 2) as weight,
  FROM(
    SELECT time, REGEXP_EXTRACT(expStr, r"(.+):.+:.+") as exp, REGEXP_EXTRACT(expStr, r".+:(.+):.+") as type, SPLIT(REGEXP_EXTRACT(expStr, r".+:.+:(.+)$"), ",") as variantWithWt
    FROM
      (
      SELECT time, SPLIT(dump, "|") as expStr
      FROM
        (SELECT protoPayload.line.time as time, REGEXP_EXTRACT(protoPayload.line.logMessage, r"YATZ_DUMP\|(.+)") as dump
        FROM {{$.Tables}}
        WHERE protoPayload.line.logMessage contains "YATZ_DUMP"
          and protoPayload.line.time < TIMESTAMP("{{$.EndTime}}")
        ORDER BY time DESC
        LIMIT 1
        )
      )
    )
  WHERE exp = "{{.Experiment}}"
  ORDER BY exp, variant
  {{end}}
`

const perUserQuery = `
{{template "select" .}}
FROM (
   {{template "eventsByUser" .}}
) as e
JOIN (
   {{template "weights" .}}
) as w on e.variant = w.variant
GROUP BY event, expvariant, weight
ORDER BY event, expvariant, weight
`

const countSelects = `
{{ define "select" }}
SELECT event, CONCAT("{{$.Experiment}}",":",e.variant) as expvariant, weight,
SUM(num) as total_events,
COUNT(*) as total_users,
ROUND(SUM(sum), 2) as total_count,
ROUND(AVG(sum), 2) as avg_per_user,
NTH(50, QUANTILES(sum, 100)) as median_per_user,
{{ end }}
`

const bothSelects = `
{{ define "select" }}
SELECT event, CONCAT("{{$.Experiment}}",":",e.variant) as expvariant, weight,
SUM(num) as total_events,
COUNT(*) as total_users,
ROUND(SUM(sum), 2) as total_count,
ROUND(AVG(sum), 2) as avg_per_user,
NTH(50, QUANTILES(sum, 100)) as median_per_user,
SUM(uniques) as total_unique_events,
ROUND(AVG(uniques), 2) as avg_uniques_per_user,
NTH(50, QUANTILES(uniques, 100)) as median_uniques_per_user,
{{ end }}
`

const uniqueSelects = `
{{ define "select" }}
SELECT event, CONCAT("{{$.Experiment}}",":",e.variant) as expvariant, weight,
COUNT(*) as total_users,
SUM(uniques) as total_unique_events,
ROUND(AVG(uniques), 2) as avg_uniques_per_user,
NTH(50, QUANTILES(uniques, 100)) as median_uniques_per_user,
{{ end }}
`
