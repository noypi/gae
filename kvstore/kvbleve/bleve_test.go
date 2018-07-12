package kvbleve

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	. "gopkg.in/check.v1"

	"time"

	"github.com/blevesearch/bleve"
	_ "github.com/blevesearch/bleve/config"
	"github.com/blevesearch/bleve/document"
	"github.com/kr/pretty"
	"github.com/noypi/gae"
	"github.com/noypi/gae/dbi/std"
	"github.com/noypi/gae/logi/std"
	"github.com/noypi/gae/registry"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
)

type MySuite struct{}

var _ = Suite(&MySuite{})

func Test(t *testing.T) { TestingT(t) }

func (suite *MySuite) TestBleve(c *C) {

	ctx, fnclose, err := aetest.NewInstance(&aetest.Options{AppID: "", StronglyConsistentDatastore: true})
	c.Assert(err, IsNil)
	defer fnclose()

	logger, err := registry.GetLogi(logi.Name, map[string]interface{}{
		"context": ctx,
	})
	c.Assert(err, IsNil)

	db, err := registry.GetDbi(dbi.Name, map[string]interface{}{
		"logger":  logger,
		"context": ctx,
		"appid":   appengine.AppID(ctx),
		"name":    "mytestdb",
	})
	c.Assert(err, IsNil)

	params := map[string]interface{}{
		"logint":    logger,
		"dbint":     db,
		"namespace": []byte("mybleve"),
	}

	indexmapping := bleve.NewIndexMapping()
	tmpdir, err := ioutil.TempDir("", "blevetest")
	c.Assert(err, IsNil)
	index, err := bleve.NewUsing(tmpdir, indexmapping, bleve.Config.DefaultIndexType, Name, params)
	c.Assert(err, IsNil)

	message := struct {
		Id   string
		From string
		Body string
	}{
		Id:   "example",
		From: "marty.schoch@gmail.com",
		Body: "bleve indexing is easy",
	}
	err = index.Index(message.Id, message)
	c.Assert(err, IsNil)

	// query
	query := bleve.NewQueryStringQuery("bleve")
	searchRequest := bleve.NewSearchRequest(query)
	searchResult, err := index.Search(searchRequest)
	c.Assert(err, IsNil)

	pretty.Println(searchResult)
	logger.Debugf("%v", getDocsFromSearchResults(searchResult, index, logger))

}

func getDocsFromSearchResults(
	results *bleve.SearchResult,
	index bleve.Index,
	logger gae.LogInt,
) [][]byte {
	docs := make([][]byte, 0)

	for _, val := range results.Hits {
		id := val.ID
		doc, _ := index.Document(id)

		rv := struct {
			ID     string                 `json:"id"`
			Fields map[string]interface{} `json:"fields"`
		}{
			ID:     id,
			Fields: map[string]interface{}{},
		}
		for _, field := range doc.Fields {
			var newval interface{}
			switch field := field.(type) {
			case *document.TextField:
				newval = string(field.Value())
			case *document.NumericField:
				n, err := field.Number()
				if err == nil {
					newval = n
				}
			case *document.DateTimeField:
				d, err := field.DateTime()
				if err == nil {
					newval = d.Format(time.RFC3339Nano)
				}
			}
			existing, existed := rv.Fields[field.Name()]
			if existed {
				switch existing := existing.(type) {
				case []interface{}:
					rv.Fields[field.Name()] = append(existing, newval)
				case interface{}:
					arr := make([]interface{}, 2)
					arr[0] = existing
					arr[1] = newval
					rv.Fields[field.Name()] = arr
				}
			} else {
				rv.Fields[field.Name()] = newval
			}
		}
		logger.Debugf("%v", rv)
		pretty.Println(rv)
		j2, _ := json.MarshalIndent(rv, "", "    ")
		docs = append(docs, j2)
	}

	return docs
}
