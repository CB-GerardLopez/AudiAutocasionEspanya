import scrapy
import csv
import time
from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging


# Creem la classe de l'aranya per extreure les url's dels cotxes
class FitxaCotxe(scrapy.Spider):
    name = "FitxaCotxe"
    
    # Establim un temps de 2 segons entre peticions
    download_delay = 2

    # URL base a analitzar
    start_urls = ["https://www.autocasion.com/coches-segunda-mano/audi-ocasion"]

    # Mètode que selecciona les URL's dels vehicles
    def parse(self, response):
        for href in response.xpath('//article[contains(@class, "anuncio")]'):
            url = response.urljoin(href.xpath('.//a/@href').extract_first())
            req = scrapy.Request(url, callback=self.parse_fitxa)
            time.sleep(2)
            yield req

        # Avancem a la següent pàgina
        next_page_url = response.xpath('//a[contains(text(), "Siguiente")]/@href').extract_first()
        if next_page_url is not None:
            yield scrapy.Request(response.urljoin(next_page_url))

    # Mètode que selecciona les informacions de cada vehicle
    def parse_fitxa(self, response):
        # Obrim l'arxiu csv de sortida de les dades
        with open("audi.csv", 'a', newline='', encoding='utf-8') as outfile:
            fieldnames = ['descripcio', 'descripcio2', 'any', 'provincia', 'kms', 'matriculacio', 'garantia', 'color',
                          'ambiental', 'preu', 'preu-nou', 'llarg', 'ample', 'alt', 'batalla', 'maleter', 'peso-max',
                          'carroceria', 'portes', 'places', 'combustible', 'cilindrada', 'cilindres',
                          'sobrealimentacio',
                          'traccio', 'transmisio', 'marxes', 'potencia-kw', 'potencia-cv', 'par', 'velocitat-max',
                          'acceleracio', 'consum-urba', 'consum-carretera', 'consum-mitja', 'co2', 'diposit',
                          'tipus-venedor',
                          'data-publicacio']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter=';')

            for item in response.xpath('//div[contains(@class, "container ficha-vo sheet")]'):
                # Creem un diccionari per recollir les dades del vehicle
                fitxa = {}

                try:
                    fitxa['descripcio'] = item.xpath('./div[@class = "titulo-ficha"]/h1/text()').extract_first().strip()
                except:
                    fitxa['descripcio'] = ""
                try:
                    fitxa['descripcio2'] = item.xpath('./div[@class = "titulo-ficha"]/h1/span/text()').extract_first()
                except:
                    fitxa['descripcio2'] = ""
                try:
                    fitxa['any'] = item.xpath('./div[@class = "titulo-ficha"]/p/span/text()')[0].extract()
                except:
                    fitxa['any'] = ""
                try:
                    fitxa['provincia'] = item.xpath('./div[@class = "titulo-ficha"]/p/span/text()')[1].extract()
                except:
                    fitxa['provincia'] = ""
                try:
                    fitxa['kms'] = item.xpath('./div[@class = "titulo-ficha"]/p/span/text()')[2].extract()
                except:
                    fitxa['kms'] = ""
                try:
                    fitxa['matriculacio'] = item.xpath('./section[@class = "col-izq"]/div/ul/li/span/text()')[1].extract()
                except:
                    fitxa['matriculacio'] = ""
                try:
                    fitxa['garantia'] = item.xpath('./section[@class = "col-izq"]/div/ul/li/span/text()')[13].extract()
                except:
                    fitxa['garantia'] = ""
                try:
                    valor = item.xpath('./section[@class = "col-izq"]/div/ul/li/span/text()')[15].extract().strip()
                    fitxa['color'] = valor.replace("\n                     /", "")
                except:
                    fitxa['color'] = ""
                try:
                    fitxa['ambiental'] = item.xpath('./section[@class = "col-izq"]/div/ul/li/span/text()')[17].extract()
                except:
                    fitxa['ambiental'] = ""
                try:
                    fitxa['preu'] = item.xpath('./section[@class = "col-izq"]/div/div[@class = "detalles-precio"]/ul/li[@class = "vo"]/span/text()').extract_first()
                except:
                    fitxa['preu'] = ""
                try:
                    fitxa['preu-nou'] = item.xpath('./section[@class = "col-izq"]/div/div[@class = "detalles-precio"]/ul/li[contains(text(), "nuevo")]/span/text()').extract_first()
                except:
                    fitxa['preu-nou'] = ""
                try:
                    valor = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-1")]/li[@class = "dimensiones"]/span[@class = "largo"]/text()').extract_first()
                    fitxa['llarg'] = valor.replace("Largo ", "")
                except:
                    fitxa['llarg'] = ""
                try:
                    valor = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-1")]/li[@class = "dimensiones"]/span[@class = "ancho"]/text()').extract_first()
                    fitxa['ample'] = valor.replace("Ancho ", "")
                except:
                    fitxa['ample'] = ""
                try:
                    valor = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-1")]/li[@class = "dimensiones"]/span[@class = "alto"]/text()').extract_first()
                    fitxa['alt'] = valor.replace("Alto ", "")
                except:
                    fitxa['alt'] = ""
                try:
                    fitxa['batalla'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-1")]/li/text()')[6].extract()
                except:
                    fitxa['batalla'] = ""
                try:
                    fitxa['maleter'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-1")]/li/text()')[7].extract()
                except:
                    fitxa['maleter'] = ""
                try:
                    fitxa['peso-max'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-1")]/li/text()')[8].extract()
                except:
                    fitxa['peso-max'] = ""
                try:
                    fitxa['carroceria'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-1")]/li/text()')[9].extract()
                except:
                    fitxa['carroceria'] = ""
                try:
                    fitxa['portes'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-1")]/li/text()')[10].extract().strip()
                except:
                    fitxa['portes'] = ""
                try:
                    fitxa['places'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-1")]/li/text()')[11].extract()
                except:
                    fitxa['places'] = ""
                try:
                    fitxa['combustible'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-2")]/li/text()')[0].extract()
                except:
                    fitxa['combustible'] = ""
                try:
                    fitxa['cilindrada'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-2")]/li/text()')[1].extract()
                except:
                    fitxa['cilindrada'] = ""
                try:
                    fitxa['cilindres'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-2")]/li/text()')[2].extract()
                except:
                    fitxa['cilindres'] = ""
                try:
                    fitxa['sobrealimentacio'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-2")]/li/text()')[3].extract()
                except:
                    fitxa['sobrealimentacio'] = ""
                try:
                    fitxa['traccio'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-3")]/li/text()')[0].extract()
                except:
                    fitxa['traccio'] = ""
                try:
                    fitxa['transmisio'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-3")]/li/text()')[1].extract()
                except:
                    fitxa['transmisio'] = ""
                try:
                    fitxa['marxes'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-3")]/li/text()')[2].extract()
                except:
                    fitxa['marxes'] = ""
                try:
                    fitxa['potencia-kw'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-4")]/li/text()')[0].extract()
                except:
                    fitxa['potencia-kw'] = ""
                try:
                    fitxa['potencia-cv'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-4")]/li/text()')[1].extract()
                except:
                    fitxa['potencia-cv'] = ""
                try:
                    fitxa['par'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-4")]/li/text()')[2].extract()
                except:
                    fitxa['par'] = ""
                try:
                    fitxa['velocitat-max'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-4")]/li/text()')[3].extract()
                except:
                    fitxa['velocitat-max'] = ""
                try:
                    fitxa['acceleracio'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-4")]/li/text()')[4].extract()
                except:
                    fitxa['acceleracio'] = ""
                try:
                    fitxa['consum-urba'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-5")]/li/text()')[0].extract()
                except:
                    fitxa['consum-urba'] = ""
                try:
                    fitxa['consum-carretera'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-5")]/li/text()')[1].extract()
                except:
                    fitxa['consum-carretera'] = ""
                try:
                    fitxa['consum-mitja'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-5")]/li/text()')[2].extract()
                except:
                    fitxa['consum-mitja'] = ""
                try:
                    fitxa['co2'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-5")]/li/text()')[3].extract()
                except:
                    fitxa['co2'] = ""
                try:
                    fitxa['diposit'] = item.xpath('./section[@class = "col-izq"]/div/div/ul[contains(@class, "tab-spec-5")]/li/text()')[4].extract()
                except:
                    fitxa['diposit'] = ""
                try:
                    fitxa['tipus-venedor'] = item.xpath('./section[@class = "col-izq"]/div/div/div[@class = "datos-concesionario"]/p[@class = "tit"]/text()').extract_first()
                except:
                    fitxa['tipus-venedor'] = ""
                # try:
                #     fitxa['nom-venedor'] = item.xpath('./section[@class = "col-izq"]/div/div/div[@class = "datos-concesionario"]/p/span/text()').extract_first()
                # except:
                #     fitxa['nom-venedor'] = ""
                try:
                    valor = item.xpath('./section[@class = "col-izq"]/div/p[@class = "ref"]/text()').extract_first().strip()
                    fitxa['data-publicacio'] = valor[-10:]
                except:
                    fitxa['data-publicacio'] = ""
                
                # Escriu la informació del vehicle al csv
                writer.writerow(fitxa)


if __name__ == "__main__":
    
    # Definim i configurem el Crawler que cercarà la informació
    configure_logging()
    runner = CrawlerRunner()

    @defer.inlineCallbacks
    def crawl():
        yield runner.crawl(FitxaCotxe)
        reactor.stop()
    
    # Llencem la cerca
    crawl()
    reactor.run()
