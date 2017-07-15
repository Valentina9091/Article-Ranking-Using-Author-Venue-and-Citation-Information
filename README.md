# Article-Ranking-Using-Author-Venue-and-Citation-Information

According to this algorithm, citation is not the only factor that determines the importance of a paper, while other information such as authors, venues are also relevant. This implementation utilizes ranking publication using information about papers, authors, and venues. The fun-damental idea is good authors write good papers, and good papers are not only cited by many others, but also published in good journals or conferences.

DATA SET AMiner-Paper.rar data set is the focus. https://aminer.org/billboard/aminernetwork

This data set contains:

#index ---- index id of this paper

#* ---- paper title

#@ ---- authors (separated by semicolons)

#o ---- affiliations (separated by semicolons, and each affiliation corresponds to an author in order)

#t ---- year

#c ---- publication venue

#% ---- the id of references of this paper (there are multiple lines, with each indicating a reference)

#! ---- abstract

Steps:
This implementation ranks articles using information about papers, authors, and venues.
The initial paper score is determined using the following equation 

![Alt text](https://github.com/Valentina9091/Article-Ranking-Using-Author-Venue-and-Citation-Information/blob/master/img/1.png "")

Next, we compute the score for each author using the paper score.

Ap - The author score is computed by averaging the scores of his published papers(Ap).

Vp-The Venue score computation based on the papers published at the venue.
Av - The author score obtained from averaging the scores of his published venues.
Ar - The refined author score is obtained from averaging the scores of his published venues and the published papers.

![Alt text](https://github.com/Valentina9091/Article-Ranking-Using-Author-Venue-and-Citation-Information/blob/master/img/2.png "")


Finally we compute paper score based on the following equations which gives considers citations, venue and author information.

![Alt text](https://github.com/Valentina9091/Article-Ranking-Using-Author-Venue-and-Citation-Information/blob/master/img/3.png "")


![Alt text](https://github.com/Valentina9091/Article-Ranking-Using-Author-Venue-and-Citation-Information/blob/master/img/4.png "")
