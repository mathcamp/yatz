package yatz

var activeExperiments = []*experiment{
	{
		Name: "experiment_name",
		Type: abTestType,
		Variations: []variation{{
			Name:   Control,
			Weight: 1,
		}, {
			Name:   "something_else",
			Weight: 1,
			Whitelist: []string{"userid1", "userid2"},
		}},
	},
}
