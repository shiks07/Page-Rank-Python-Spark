# Template for writing MapReduce programs using mrjob
# % python mrjob-pagerank.py <input file>  -q

from mrjob.job import MRJob
from mrjob.step import MRStep

class MR_program(MRJob):

    def configure_args(self):
        super(MR_program, self).configure_args()
        self.add_passthru_arg('--nodes')    ##  <--- specify which new command line args are used
        self.add_passthru_arg('--beta')    ##  <--- specify which new command line args are used
        self.add_passthru_arg('-N')    ##  <--- specify which new command line args are used

    def mapper_1(self, _, line):
        num_nodes = int(self.options.nodes)
        init_pr = 1/num_nodes    ##  <--- initializing the page ranks
        y,*x = line.strip().split()    ##  <--- obtaining the page and its out_links 
        for out_link in x:
            yield out_link, init_pr/len(x)    ##  <--- yielding marginal pagerank of the page for each of its outlinks
        yield y,x    ## <--- Also yielding out_links of the page

    def reducer_1(self, x, pr_y_by_n_or_nbrs_of_x):  
        num_nodes = int(self.options.nodes)
        beta = float(self.options.beta)
        lst = list(pr_y_by_n_or_nbrs_of_x)
        pr_x = (1-beta)/num_nodes    ## <--- probability of randomly coming on the page through typing in address bar
        for item in lst:
            if type(item) is float:    ## <--- checking if the value in the list is one of the pageranks
                pr_x += beta*item    ## <--- adding the probablity of landing on page by clicking on link in each of its in-links
            else:
                out_links = item
        for z in out_links:
            yield(z,pr_x/len(out_links))    ## <--- for each out_link, yielding marginal page rank
        print (x,pr_x)
        yield x,out_links    ## <--- Also yielding out_links of the page

    def reducer_2(self, x, pr_y_by_n_or_nbrs_of_x):  
        num_nodes = int(self.options.nodes)
        beta = float(self.options.beta)
        lst = list(pr_y_by_n_or_nbrs_of_x)    
        pr_x = (1-beta)/num_nodes
        for item in lst:
            if type(item) is float:
                pr_x += beta*item
        yield x, pr_x    ## <--- yielding final pageranks of each page
            
            

    def steps(self):
        N = int(self.options.N)
        return [MRStep(mapper=self.mapper_1)] + \
               [MRStep(reducer=self.reducer_1)]*N + \
               [MRStep(reducer=self.reducer_2)]
               

if __name__ == '__main__':
    # change to match the name of the class
    MR_program.run()
    
