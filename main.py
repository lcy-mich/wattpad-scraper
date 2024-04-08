from threading import Thread
from queue import Queue
from requests import get
from os import mkdir as makedir, path
from bs4 import BeautifulSoup
from time import sleep
from sys import argv

mkdir = lambda x : (print(f"creating directory \"{x}\""), makedir(x)) if not path.exists(x) else 0

err_msg = "ERROR ({error}) occurred when running with args: {args}\n{message}"
log_temp = "{action} {itemtype} {item} {url}, {extra}"

downloadpath = "./Scraped/"

mkdir(downloadpath)
    
failedidpath = "Failed_IDs.txt"
storyurl = "https://www.wattpad.com/story/{id}"
chapterurl = "https://www.wattpad.com{id}/page/{page}"
startid = 95
endid = 301339259        
maxretries = 10

defaultsleeptime=120
sleepdec=defaultsleeptime//4

illegalchars = "#$%!\'\":@+`|={}\\<>*?/"

strip_chars = lambda string : ''.join(char for char in string if char not in illegalchars)

headers = {"User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'}

def _threadsplitter(threadnum, startidx, endidx):
    idxdiff = (endidx-startidx)
    unadjloopamt = (idxdiff//threadnum)
    
    loopamt = unadjloopamt + (1 if idxdiff%threadnum != 0 else 0)
    leftover = endidx - threadnum*unadjloopamt
    
    failedids = Queue()
    
    def _fetchhtml(id):
        print(f"Attempting to fetch {id}")
        if type(id)==int:
            return get(storyurl.format(id=id), headers=headers)
        return get(id, headers=headers)

    def _processhtml(html, id, sleeptime):
        html = BeautifulSoup(html, 'html.parser')
        title = strip_chars(html.find("span",class_="sr-only").get_text().replace(" ","-"))+f"_{id}"
        desc = html.find("pre", class_="description-text").get_text()
        tags = [a.get_text() for a in html.find("ul", class_="tag-items").find_all("a")]
        chapters = [(chapter.find("div", class_="part-title").get_text(), chapter.get("href")) for chapter in html.find("div", class_="story-parts").find("ul").find_all("a")]
        mkdir(downloadpath+title)
        
        
        with open(path.abspath(downloadpath+title+"/tags.txt"), "w", encoding="utf-8") as f:
            f.write(", ".join(tags))
        
        with open(f"{downloadpath}{title}/description.txt", "w", encoding="utf-8") as f:
            f.write(desc)
        
        for (chapter, link) in chapters:
            if path.exists(f"{downloadpath}{title}/{strip_chars(chapter)}.txt"):
                print(log_temp.format(action="SKIPPED", itemtype="chapter", item=chapter, url=url))
                continue
            pages=[]
            pagecount=1
            isempty = False
            while not isempty:
                url = chapterurl.format(id=link,page=pagecount)
                chapterhtml = _fetchhtml(url)
                retries = 0
                while chapterhtml.status_code != 200 and retries <= maxretries:
                    retries += 1
                    print(log_temp.format(action="FAILURE", itemtype="chapter", item=chapter, url=url, extra=f"STATUS_CODE: {chapterhtml.status_code} RETRYING WITH DELAY {sleeptime}"))
                    if chapterhtml.status_code != 404:
                        sleeptime += defaultsleeptime
                        sleep(sleeptime)
                    chapterhtml = _fetchhtml(url)
                chapterhtml = BeautifulSoup(chapterhtml.content, "html.parser")
                sleeptime = max(defaultsleeptime, sleeptime - sleepdec)
                
                if chapterhtml.find("div", class_="panel-reading").find("pre").get_text().strip() == "":
                    isempty = True
                    
                print(log_temp.format(action="SUCCESS", itemtype="chapter", item=chapter, extra=f"page {pagecount}"))
                pages.append("\n".join([p.get_text() for p in chapterhtml.find("div", class_="panel-reading").find_all("p")]))
                pagecount+=1
            print(strip_chars(chapter))
            with open(f"{downloadpath}{title}/{strip_chars(chapter)}.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(pages))
            sleep(sleeptime)
        
        return html
    
    def threadhandler(q, start, end):
        sleeptime = defaultsleeptime+threadnum//2
        for id in range(start,end+1):
            try:
                html = _fetchhtml(id)
                if html.status_code == 200:
                    print(log_temp.format(action="SUCCESS", itemtype="story", item=id))
                    sleeptime = max(defaultsleeptime, sleeptime - sleepdec)
                    _processhtml(html.content, id, sleeptime)
                else:
                    if html.status_code != 404:
                        print(log_temp.format(action="FAILURE", itemtype="story", item=id, extra=f"STATUS_CODE: {html.status_code} RETRYING WITH DELAY {sleeptime}"))
                        q.put(id)
                        sleeptime += defaultsleeptime
                        sleep(sleeptime)
                        continue
                    print(log_temp.format(action="SKIPPED", itemtype="story", item=id))
            except:
                    q.put(id)
    
    threads = []
    if threadnum < idxdiff:
        for i in range(threadnum):
            print(f"adding thread spanning ids {startidx+i*loopamt}-{startidx+(i+1)*loopamt}")
            threads.append(Thread(target=threadhandler, args=(failedids, startidx+i*loopamt, startidx+(i+1)*loopamt)))
        if unadjloopamt != loopamt:
            print(f"adding thread spanning ids {endidx-leftover}-{endidx}")
            threads.append(Thread(target=threadhandler, args=(failedids, endidx-leftover, endidx)))
        else:
            print(f"adding thread spanning ids {endidx-loopamt}-{endidx}")
            threads.append(Thread(target=threadhandler, args=(failedids, endidx-loopamt, endidx)))
    else:
        for i in range(startidx, endidx+1):
            print(f"adding thread for id {i}")
            threads.append(Thread(target=threadhandler, args=(failedids, i, i)))
            
    for thread in threads:
        thread.start()
        sleep(defaultsleeptime)
    for thread in threads:
        thread.join()
    failedids.join()
    
    if failedids.empty():
        return
    
    with open(failedidpath, "a", encoding="utf-8") as f:
        while not failedids.empty():
            failedid = failedids.get()
            print(log_temp.format(action="FAILURE", itemtype="ID", item=failedid, extra=f"Writing {failedidpath}"))
            f.write(str(id))
    return

def cmdlist():
    print("Here are all the valid commands: ")
    for command in commands:
        handler = commands[command]
        print(f"-> \"{command}{' (args)' if handler.__code__.co_argcount > 0 else ''}\" for {handler.__qualname__}")

def scrape(startidx=startid,endidx=endid, threadnum=1000):
    startidx, endidx, threadnum = int(startidx), int(endidx), int(threadnum)
    if endidx < startidx: raise Exception("Ending ID cannot be lower than starting ID")
    _threadsplitter(threadnum, startidx, endidx)
    print(f"scrape spanning ids {startidx}-{endidx} has finished. please check {failedidpath} for list of ids failed to fetch")
    return

commands = {
    "-h":cmdlist,
    "scrape":scrape
    }

def main(args):
    try:
        if args[0] in commands:
            if commands[args[0]].__code__.co_argcount > 0:
                commands[args[0]](*args[1::])
            else:
                commands[args[0]]()
            return
        raise Exception("Invalid commands")
    except Exception as e:
        print(err_msg.format(error=e, args=", ".join(args), message="Try running \"-h\" to see all commands."))
        print(log_temp.format(action="EXCEPTION", itemtype=e))
        return

if __name__ == "__main__":
    main(argv[1::])
