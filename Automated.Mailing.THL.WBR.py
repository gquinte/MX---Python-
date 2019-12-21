import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from pathlib import Path
import os

datas = pd.read_csv('SIMDATA.csv')
logins = datas['Assignee Identity'].unique()

#sdata = pd.read_csv('subtasks.csv')
#slogins = sdata['Owner'].unique()

for i in logins:
    #DataFrameforAdditionalRequests
    reportcolumns = ['Title','Issue Url','Assignee Identity','Days Open','ETA','Priority and SLA','Days Past ETA']
    dfpre = datas[reportcolumns]
    df = dfpre.loc[dfpre['Assignee Identity']==i]
    df.reset_index(inplace=True, drop=True)

    #Define top offender SIM per owner
    pa = df['Days Past ETA'].max()

    #Dictionary for escalations
    emanager = pd.read_csv('peoplemanager.csv')
    L1 = emanager['login']
    L2 = emanager['manager']
    L3 = emanager['sponsor']
    d = {k: v for k, v in zip(L1, L2)}
    d2 = {k: v for k, v in zip(L1, L3)}
    #print(d)
    #print(d2)
    #print(d.get(i))
    #print(d2.get(i))

    m = d.get(i)
    s = d2.get(i)

    # Create dataframe for escalations
    dataem = [['56', 'Notify CatLead (jifernan)'], ['28', 'Notify Sponsor (' + s + ')'], ['0', 'Notify Manager (' + m + ')']]
    em = pd.DataFrame(dataem,
                      columns=['Days past ETA', 'Action'])

    # Set dataframe as HTML
    # HTML
    styles = [
        dict(selector="th", props=[("background-color", "#006600"),("color", "#ffffff")]),
        dict(selector="th", props=[("font-size", "14"), ("text-align", "left"), ("font-family","Calibri"),("border-style","solid"),("border-collapse", "collapse"),("padding", "5px")]),
        dict(selector="td", props=[("font-size", "14"), ("text-align", "left"), ("font-family","Calibri"),("border-style","solid"),("border-collapse", "collapse"),("padding", "5px")])
    ]
    html = (df.style.set_table_styles(styles).set_table_attributes("border=2").format({"DATE": "{:%Y-%m-%d}"}).render())
    html2 = (em.style.set_table_styles(styles).set_table_attributes("border= 2").format({"DATE": "{:%Y-%m-%d}"}).render())

    # ****************Email****************
    commaspace = "; "

    sender = "gquinte@amazon.com"
    if pa >= 56:
        recipients = (i + '@amazon.com', s + '@amazon.com', m + '@amazon.com', 'jifernan@amazon.com', 'gquinte@amazon.com')
    elif pa < 56 and pa >= 28:
        recipients = (i+'@amazon.com', m + '@amazon.com', s + '@amazon.com', 'gquinte@amazon.com')
    else:
        recipients = (i+'@amazon.com', m + '@amazon.com', 'gquinte@amazon.com')

    # Create the enclosing (outer) message
    outer = MIMEMultipart()
    outer["Subject"] = "[ACTION REQUIRED] THL WBR OPEN SIMS, OWNER:  " + i
    outer["To"] = commaspace.join(recipients)
    outer["From"] = sender
    outer.preamble = "You will not see this in a MIME-aware mail reader.\n"

    # Create the body of the message.
    report =  """\
    <html>
      <head></head>
      <body>
           </p> 
           <p style = "font-family:Calibri,serif;font-size:14px;"> 
           Hello  {i}!
            </p>
      </body>
    </html>
    """.format(i=i)
    #report +=  "<br><br>"
    #report += "Slide owner manager:   " + m
    #report += "<br>"
    #report += "Slide sponsor:   " + s
    report += """\
    <html>
      <head></head>
      <body>
           </p> 
           <p style = "font-family:Calibri,serif;font-size:14px;">           
           You are receiving this message because you are owner of one or more open SIMs for the THL WBR<br>
           <br>
           SIMs for additional requirements, for which you are currently owner and have missed the ETA set on SIM, are enlisted on the following table:<br>
          </p>
      </body>
    </html>
    """
    report += html
    report += """\
       <html>
         <head></head>
         <body>
              </p> 
              <p style = "font-family:Calibri,serif;font-size:14px;"> 
              <br>        
              If SIMs remain open with an expired ETA, reports will be escalated as specified on the following table: <br>
              <br>
             </p>
         </body>
       </html>
       """
    report += html2
    report += """\
    <html>
      <head></head>
      <body>
        <p>
         <p style = "font-family:Calibri,serif;font-size:14px;"> 
           Please reply to this mail with the ETA for this SIM(s) to be solved. Reports will be sent on a weekly basis.<br>
           <br>
           If there is a mistake on any of the information contained on this report, or need any additional change, please contact @gquinte<br>
           <br>
           Additional information about slide ownership can be found <a href="onenote:///\\ant.amazon.com\dept-na\MEX13\Retail-CE\MX_CE\WBR.one#WBR%20DECK%20-%20Index%20and%20ownership%20&section-id={10BEAC8D-43DA-409F-886C-DFB5D4FF939D}&page-id={D947906F-4586-44A7-B59E-2527553CF55E}&end">here</a><br>
           <br>
           Here is the <a href="https://sim.amazon.com/issues/create?template=8469337b-b6e0-4f54-808d-07be5e0a479b">link</a> to make a new request to the THL WBR.<br>
           <br>
           Regards!<br>
           Guillermo 
           </p>
      </body>
    </html>
    """

    # Record the MIME types.
    done = MIMEText(report, "html")

    # Attach parts into message container
    outer.attach(done)
    composed = outer.as_string()

    # Send the email
    try:
        with smtplib.SMTP("mail-relay.amazon.com") as s:
            s.ehlo()
            s.starttls()
            s.ehlo()
            s.sendmail(sender, recipients, composed)
            s.close()
            print("Email sent!" + i)
    except:
        print("Unable to send the email. Error: ", sys.exc_info()[0])
        raise

print("END, all slide owners have been notified")