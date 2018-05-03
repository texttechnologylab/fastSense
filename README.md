# fastSense
An Efficient Word Sense Disambiguation Classifier

## Cite
T. Uslu, A. Mehler, D. Baumartz, A. Henlein, and W. Hemati, “fastSense: An Efficient Word Sense Disambiguation Classifier,” in Proceedings of the 11th edition of the Language Resources and Evaluation Conference, May 7 – 12, Miyazaki, Japan, 2018. accepted

### BibTeX

```
@InProceedings{Uslu:et:al:2018,
  Author         = {Tolga Uslu and Alexander Mehler and Daniel Baumartz
                   and Alexander Henlein and Wahed Hemati },
  Title          = {fastSense: An Efficient Word Sense Disambiguation
                   Classifier},
  BookTitle      = {Proceedings of the 11th edition of the Language
                   Resources and Evaluation Conference, May 7 - 12},
  Series         = {LREC 2018},
  Address        = {Miyazaki, Japan},
  Note           = {accepted},
  pdf            = {https://www.texttechnologylab.org/wp-content/uploads/2018/03/fastSense.pdf},
  year           = 2018
}
```

## Using the REST API

To use the fastSense REST API, perform a `POST` request to https://textimager.hucompute.org/fastsense/disambiguate with the following JSON data:

```
{
	"inputText": "Your input text"
}
```
