module Minhash
{
    authentication optional;

    funcdef compute_genome_distance_for_genome(string genome_id,
				    float max_pvalue,
				    float max_distance,
				    int max_hits,
				    int include_reference,
				    int include_representative)
	returns (list<tuple<string genome_id,
		 	    float distance,
		 	    float pvalue,
		 	    string counts>>);

    funcdef compute_genome_distance_for_fasta(string ws_fasta_path,
					      float max_pvalue,
					      float max_distance,
					      int max_hits,
					      int include_reference,
					      int include_representative)
	returns (list<tuple<string genome_id,
		 float distance,
		 float pvalue,
		 string counts>>);

};
