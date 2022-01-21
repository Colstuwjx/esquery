package esquery

//----------------------------------------------------------------------------//

// TermsAggregation represents an aggregation of type "terms", as described in
// https://www.elastic.co/guide/en/elasticsearch/reference/current/
//      search-aggregations-bucket-terms-aggregation.html
type TermsAggregation struct {
	name        string
	field       string
	size        *uint64
	shardSize   *float64
	showTermDoc *bool
	aggs        []Aggregation
	order       map[string]string
	include     []string
}

// TermsAgg creates a new aggregation of type "terms". The method name includes
// the "Agg" suffix to prevent conflict with the "terms" query.
func TermsAgg(name, field string) *TermsAggregation {
	return &TermsAggregation{
		name:  name,
		field: field,
	}
}

// Name returns the name of the aggregation.
func (agg *TermsAggregation) Name() string {
	return agg.name
}

// Size sets the number of term buckets to return.
func (agg *TermsAggregation) Size(size uint64) *TermsAggregation {
	agg.size = &size
	return agg
}

// ShardSize sets how many terms to request from each shard.
func (agg *TermsAggregation) ShardSize(size float64) *TermsAggregation {
	agg.shardSize = &size
	return agg
}

// ShowTermDocCountError sets whether to show an error value for each term
// returned by the aggregation which represents the worst case error in the
// document count.
func (agg *TermsAggregation) ShowTermDocCountError(b bool) *TermsAggregation {
	agg.showTermDoc = &b
	return agg
}

// Aggs sets sub-aggregations for the aggregation.
func (agg *TermsAggregation) Aggs(aggs ...Aggregation) *TermsAggregation {
	agg.aggs = aggs
	return agg
}

// Order sets the sort for terms agg
func (agg *TermsAggregation) Order(order map[string]string) *TermsAggregation {
	agg.order = order
	return agg
}

// Include filter the values for  buckets
func (agg *TermsAggregation) Include(include ...string) *TermsAggregation {
	agg.include = include
	return agg
}

// Map returns a map representation of the aggregation, thus implementing the
// Mappable interface.
func (agg *TermsAggregation) Map() map[string]interface{} {
	innerMap := map[string]interface{}{
		"field": agg.field,
	}

	if agg.size != nil {
		innerMap["size"] = *agg.size
	}
	if agg.shardSize != nil {
		innerMap["shard_size"] = *agg.shardSize
	}
	if agg.showTermDoc != nil {
		innerMap["show_term_doc_count_error"] = *agg.showTermDoc
	}
	if agg.order != nil {
		innerMap["order"] = agg.order
	}

	if agg.include != nil {
		if len(agg.include) <= 1 {
			innerMap["include"] = agg.include[0]
		} else {
			innerMap["include"] = agg.include
		}

	}

	outerMap := map[string]interface{}{
		"terms": innerMap,
	}
	if len(agg.aggs) > 0 {
		subAggs := make(map[string]map[string]interface{})
		for _, sub := range agg.aggs {
			subAggs[sub.Name()] = sub.Map()
		}
		outerMap["aggs"] = subAggs
	}

	return outerMap
}

//----------------------------------------------------------------------------//

// DateHistogramAggregation represents an aggregation of type "date_histogram", as described in
// https://www.elastic.co/guide/en/elasticsearch/reference/current/
//      search-aggregations-bucket-datehistogram-aggregation.html
type DateHistogramAggregation struct {
	name             string
	field            string
	calendarInterval string
	fixedInterval    string
	format           string
	offset           string
	keyed            *bool
	minDocCount      *uint64
	missing          string
	order            map[string]string

	aggs []Aggregation
}

// DateHistogramAgg creates a new aggregation of type "date_histogram".
func DateHistogramAgg(name, field string) *DateHistogramAggregation {
	return &DateHistogramAggregation{
		name:  name,
		field: field,
	}
}

// Name returns the name of the aggregation.
func (agg *DateHistogramAggregation) Name() string {
	return agg.name
}

// Aggs sets sub-aggregations for the aggregation.
func (agg *DateHistogramAggregation) Aggs(aggs ...Aggregation) *DateHistogramAggregation {
	agg.aggs = aggs
	return agg
}

// CalendarInterval sets calendarInterval
func (agg *DateHistogramAggregation) CalendarInterval(interval string) *DateHistogramAggregation {
	agg.calendarInterval = interval
	return agg
}

// Fixedinterval sets fixedInterval
func (agg *DateHistogramAggregation) Fixedinterval(interval string) *DateHistogramAggregation {
	agg.fixedInterval = interval
	return agg
}

// Format sets format
func (agg *DateHistogramAggregation) Format(format string) *DateHistogramAggregation {
	agg.format = format
	return agg
}

// Offset sets offset
func (agg *DateHistogramAggregation) Offset(offset string) *DateHistogramAggregation {
	agg.offset = offset
	return agg
}

// Order sets the sort for terms agg
func (agg *DateHistogramAggregation) Order(order map[string]string) *DateHistogramAggregation {
	agg.order = order
	return agg
}

// Keyed sets keyed is true or false
func (agg *DateHistogramAggregation) Keyed(keyed bool) *DateHistogramAggregation {
	agg.keyed = &keyed
	return agg
}

// Missing sets missing value
func (agg *DateHistogramAggregation) Missing(missing string) *DateHistogramAggregation {
	agg.missing = missing
	return agg
}

// MinDocCount sets min doc count
func (agg *DateHistogramAggregation) MinDocCount(minDocCount uint64) *DateHistogramAggregation {
	agg.minDocCount = &minDocCount
	return agg
}

// Map returns a map representation of the aggregation, thus implementing the
// Mappable interface.
func (agg *DateHistogramAggregation) Map() map[string]interface{} {
	innerMap := map[string]interface{}{
		"field": agg.field,
	}

	if agg.calendarInterval != "" {
		innerMap["calendar_interval"] = agg.calendarInterval
	}

	if agg.fixedInterval != "" {
		innerMap["fixed_interval"] = agg.fixedInterval
	}

	if agg.format != "" {
		innerMap["format"] = agg.format
	}

	if agg.offset != "" {
		innerMap["offset"] = agg.offset
	}

	if agg.missing != "" {
		innerMap["missing"] = agg.missing
	}

	if agg.minDocCount != nil {
		innerMap["min_doc_count"] = agg.minDocCount
	}

	if agg.keyed != nil {
		innerMap["keyed"] = *agg.keyed
	}

	if agg.order != nil {
		innerMap["order"] = agg.order
	}

	outerMap := map[string]interface{}{
		"date_histogram": innerMap,
	}

	if len(agg.aggs) > 0 {
		subAggs := make(map[string]map[string]interface{})
		for _, sub := range agg.aggs {
			subAggs[sub.Name()] = sub.Map()
		}
		outerMap["aggs"] = subAggs
	}

	return outerMap
}
