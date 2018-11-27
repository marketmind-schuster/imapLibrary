using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;
using System.Diagnostics;

using MailKit;
using MimeKit;
using MailKit.Net.Imap;

using System.IO;


namespace imapLibrary
{

    public class imap
    {
        private static string serverPfad = @"\\svrintweb.marketmind.at\wwwroot$\elws.marketmind.at\broman";

        //für Mailpasswörter
        public static string pwdMailEncryptPassphrase = "!Passphrase#fuer#Imap#Postfächer!";

        public static string folderNameAutoreply = "Auto-Reply";

        //eine Antwort innerhalb von wievielen Sekunden wird als Auto-Reply klassifiziert?
        public static int maxSekundenAutoreply = 30;


        private static bool DataTableIsNullOrEmpty(DataTable dt)
        {
            if (dt == null) return true;
            if (dt.Rows == null) return true;
            if (dt.Rows.Count == 0) return true;
            return false;
        }



        /*
         * Fehlermeldungen ins Log-File
         */

        private static void Log(tsLib.dbConnector dbcon, int mailadrID, string fehlerText)
        {
            Debug.WriteLine(fehlerText);

            dbcon.executeNonQuery(@"
                    INSERT INTO t_ReplyLog (mailadrID, logText, logDate)
                    VALUES (@mailadrID, @logText, GETDATE())
                "
                , new object[,]{
                    {"@mailadrID", mailadrID}
                    , {"@logText", fehlerText}
                });

        } //ENDE Log



        /*
         * neues Mailpasswort anlegen
         */

        public static string CreatePassword(int length)
        {
            const string valid = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890#!()+";
            StringBuilder res = new StringBuilder();
            Random rnd = new Random();
            while (0 < length--)
            {
                res.Append(valid[rnd.Next(valid.Length)]);
            }
            return res.ToString();

        } //ENDE CreatePassword




        /*
            extrahiert die Mailadressen aus einer InternetAdressList und fügt sie zu einem String zusammen
        */

        private static string internetAddressListToString(MimeKit.InternetAddressList addrList)
        {
            StringBuilder sb = new StringBuilder();

            foreach (var mailbox in addrList.Mailboxes)
            {
                if (mailbox.Address.Length == 0) continue;

                if (sb.Length > 0) sb.Append(",");
                sb.Append(mailbox.Address);
            }

            return sb.ToString(); ;
        }



        /*
         * aktualisiert die Flags "Seen" und "Answered"
         * Rückgabewert ist der höchste Wert von ModSeq (sprich die letzte Änderung am Server)
         */

        private static void folder_updateFlags(tsLib.dbConnector dbcon, int mailadrID, IMailFolder folder)
        {
            int mailID = -1, modSeq = -1;
            bool seen = false, answered = false;


            //welcher Folder ist das? anlegen falls nicht vorhanden
            int folderID = tsLib.tsConverter.objectToInt(dbcon.executeScalar(@"
                IF (SELECT COUNT(*) FROM t_ReplyFolders WHERE mailadrID = @mailadrID AND folderName = @folderName) = 0
                BEGIN
                    INSERT INTO t_ReplyFolders(mailadrID, folderName)
                    VALUES (@mailadrID, @folderName);

                    SELECT @@IDENTITY;
                END
                ELSE 
                BEGIN
                    SELECT TOP 1 folderID 
                    FROM t_ReplyFolders 
                    WHERE mailadrID = @mailadrID AND folderName = @folderName
                END
                "
                , new object[,] {
                                {"@mailadrID", mailadrID}
                                , {"@folderName", folder.Name}
                             }
                ));

            if (folderID <= 0)
            {
                Log(dbcon, mailadrID, "Fehler beim Anlegen des Folders '" + folder.Name + "' in der Datenbank");
                return;
            }

            int maxModSeq = tsLib.tsConverter.objectToInt(dbcon.executeScalar(@"
                SELECT maxModSeq 
                FROM t_ReplyFolders
                WHERE folderID = " + folderID));

            //ab dem nächsten Index zu prüfen anfangen
            if (maxModSeq > 0) maxModSeq++;


            //Suche nach Änderungen, die noch nicht in die Datenbank übertragen wurden
            MailKit.Search.SearchQuery searchQuery = maxModSeq > 0 ? MailKit.Search.SearchQuery.ChangedSince((ulong)maxModSeq) : MailKit.Search.SearchQuery.All;


            //Zugriff auf den Folder
            folder.Open(FolderAccess.ReadOnly);

            var changed = folder.Search(searchQuery);
            if (changed == null) return;
            if (changed.Count == 0) return;


            var summaries = folder.Fetch(changed, MessageSummaryItems.Flags | MessageSummaryItems.ModSeq);

            foreach (var summary in summaries)
            {
                var message = folder.GetMessage(summary.UniqueId);
                if (message.MessageId == null) continue;


                mailID = tsLib.tsConverter.objectToInt(dbcon.executeScalar(@"
                        SELECT mailID FROM t_Reply WHERE mailadrID = @mailadrID AND messageID = @messageID"
                            , new object[,] {
                                {"@mailadrID", mailadrID}
                                , {"@messageID", message.MessageId.ToString()}
                             }
                    ));

                if (mailID <= 0) continue;


                seen = false; answered = false;
                if (summary.Flags.HasValue)
                {
                    seen = (summary.Flags.Value.ToString().ToLower().IndexOf("seen") >= 0);
                    answered = (summary.Flags.Value.ToString().ToLower().IndexOf("answered") >= 0);
                }

                dbcon.executeNonQuery(@"
                    UPDATE t_Reply
                    SET 
                        seen = @seen
                        , answered = @answered
                        , folder = @folder
                        , uid = @uid
                    WHERE mailID = @mailID"

                    , new object[,] {
                        {"@seen", seen}
                        , {"@answered", answered}
                        , {"@folder", folder.Name}
                        , {"@uid", tsLib.tsConverter.objectToInt(summary.UniqueId.ToString())}
                        , {"@mailID", mailID}
                    }
                );

                modSeq = tsLib.tsConverter.objectToInt(summary.ModSeq);
                if (modSeq > maxModSeq) maxModSeq = modSeq;

                // Debug.WriteLine(summary.UniqueId + " | " + summary.ModSeq + " | " + summary.Flags.ToString());
            }


            //Update zurück in die Datenbank
            dbcon.executeNonQuery(@"
                UPDATE t_ReplyFolders
                SET 
                    maxModSeq = @maxModSeq
                WHERE folderID = @folderID"

                , new object[,] {
                    {"@maxModSeq", maxModSeq}
                    , {"@folderID", folderID}
                }
            );

            Debug.WriteLine(folder.Name + " - maxModSeq? " + maxModSeq);

        } //ENDE folder_updateFlags



        /*
         * Attachment abspeichern
         */

        private static int saveAttachment(tsLib.dbConnector dbcon, MimeEntity attachment, int mailadrID, string email, int mailID, bool inlineImage)
        {
            Debug.WriteLine("Attachment? " + attachment.ContentId + " | " + attachment.ContentType);

            var fileName = attachment.ContentDisposition != null ? attachment.ContentDisposition.FileName : attachment.ContentType.Name;
            if (fileName == null) return -1;

            fileName = fileName.Trim();
            if (fileName.Length == 0) return -1;


            int atmtID = tsLib.tsConverter.objectToInt(dbcon.executeScalar(@"
                INSERT INTO t_ReplyAttachments 
                (
                    mailID, mailadrID
                    , isInlineImage
                    , ContentType, ContentMediaType, ContentID
                )
                VALUES
                (
                    @mailID, @mailadrID
                    , @isInlineImage
                    , @ContentType, @ContentMediaType, @ContentID
                );

                SELECT @@IDENTITY;
            "
                , new object[,] {
                    {"@mailID", mailID}
                    , {"@mailadrID", mailadrID}
                    , {"@isInlineImage", inlineImage}
                    , {"@ContentType", attachment.ContentType.ToString() ?? ""}
                    , {"@ContentMediaType", attachment.ContentType.MediaType ?? ""}
                    , {"@ContentID", attachment.ContentId ?? ""}
            }));

            if (atmtID < 0)
            {
                Log(dbcon, mailadrID, "Fehler beim Anlegen des Attachments");
                return -1;
            }


            //eigentlicher Download + Verknüpfung
            try
            {
                using (var stream = File.Create(serverPfad + "\\" + email + "\\" + mailID + "_" + fileName))
                {
                    if (attachment is MessagePart)
                    {
                        var rfc822 = (MessagePart)attachment;

                        rfc822.Message.WriteTo(stream);
                    }
                    else
                    {
                        var part = (MimePart)attachment;

                        part.ContentObject.DecodeTo(stream);
                    }
                }

                dbcon.executeNonQuery(@"
                    UPDATE t_ReplyAttachments
                    SET
                        filename = @filename
                    WHERE atmtID = @atmtID;

                    UPDATE t_Reply
                    SET HasAttachments = 1
                    WHERE mailID = @mailID;
                "
                    , new object[,] {
                        {"@filename", email + "\\" + mailID + "_" + fileName}
                        , {"@atmtID", atmtID}
                        , {"@mailID", mailID}
                    });


            }
            catch (Exception ex)
            {
                //TODO: Log --> Fehler beim Download
                dbcon.executeNonQuery("UDPATE t_ReplyAttachments SET downloadFehler = 1 WHERE atmtID = " + atmtID);
                Log(dbcon, mailadrID, "Download-Fehler: " + ex.Message);
                return -1;
            }

            return atmtID;

        } //ENDE saveAttachment




        /*
         * lädt alle Mails für einen Ordner
         * 
         * Rückgabewert = wurde ein Update durchgefüht?
         */

        private static bool folder_getMails(tsLib.dbConnector dbcon, int mailadrID, string email, IMailFolder folder, MailKit.Search.SearchQuery searchQuery = null)
        {
            try
            {
                if (!Directory.Exists(serverPfad + "\\" + email)) Directory.CreateDirectory(serverPfad + "\\" + email);
            }
            catch (Exception ex)
            {
                Log(dbcon, mailadrID, "Fehler beim Anlegen des Ordners: " + ex.Message);
                return false;
            }



            int mailID = -1, inReplyTo_MailID = -1, ReaktionszeitSekunden = -1;
            string inReplyTo = "", mailFrom = "", mailTo = "";
            bool update = false;
            DataTable dtKundendaten = null;
            DateTime? versandDatum = null;

            //Zugriff auf den Folder
            folder.Open(FolderAccess.ReadWrite);

            IMailFolder moveFolder = null;

            if (folder.Name != folderNameAutoreply)
            {
                //Ordner suchen, in den Automatische Replys kopiert werden können
                foreach (var tempFolder in folder.ParentFolder.GetSubfolders(false))
                {
                    if (tempFolder.Name == folderNameAutoreply)
                    {
                        moveFolder = tempFolder;
                        break;
                    }
                }

                //falls kein Ordner gefunden --> anlegen
                if (moveFolder == null)
                {
                    folder.ParentFolder.Create(folderNameAutoreply, true);
                }
            }



            //Suche nach neuen Mails
            foreach (var uid in folder.Search((searchQuery ?? MailKit.Search.SearchQuery.All).And(MailKit.Search.SearchQuery.NotDeleted)))
            {
                ReaktionszeitSekunden = -999;
                versandDatum = null;


                var message = folder.GetMessage(uid);


                //Mails ausfiltern, bei denen der Betreff nach automatischer Antwort aussieht
                if (message.Subject != null && moveFolder != null)
                {
                    if (
                        message.Subject.ToLower().StartsWith("automatic")
                        || message.Subject.ToLower().StartsWith("automatisch")
                        || message.Subject.ToLower().IndexOf("out of office") >= 0
                        || message.Subject.ToLower().StartsWith("autoreply")
                        || message.Subject.ToLower().StartsWith("automatisk")
                        || message.Subject.ToLower().StartsWith("otomatik")
                        || message.Subject.ToLower().StartsWith("automatyczna")
                        || message.Subject.ToLower().StartsWith("autosvar")
                        || message.Subject.ToLower().IndexOf("delivery status notification") >= 0
                        || message.Subject.ToLower().StartsWith("unzustellbar")
                        || message.Subject.ToLower().StartsWith("abwesend")
                        || message.Subject.ToLower().StartsWith("absence")
                        )
                    {
                        folder.MoveTo(uid, moveFolder);
                        continue;
                    }
                }


                //im den meisten Fällen wirds nur eine mailTo-Adresse geben
                mailTo = "";
                foreach (var mailbox in message.To.Mailboxes)
                {
                    if (mailbox.Address.IndexOf("marketmind") > 0)
                    {
                        mailTo = mailbox.Address;
                        break;
                    }
                }

                //erste Absenderadresse ist die, nach der wir in der DB suchen
                mailFrom = "";
                foreach (var mailbox in message.From.Mailboxes)
                {
                    mailFrom = mailbox.Address;
                    break;
                }


                Debug.WriteLine("FOLDER: " + folder.Name + " || FROM: " + mailFrom + " --> TO: " + mailTo);


                //in den Kundendaten von den Aussendungen nach passenden Mails suchen
                dtKundendaten = dbcon.SqlToDataTable(@"

                    --bis dahin wird rückwirkend geprüft
                    DECLARE @Date_minus90Tage datetime;
                    SET @Date_minus90Tage = DATEADD(DAY, -90, @Date);

                    --Toleranz für 'zukünftige Befragungen' (könnte sich um ein paar Sekunden überschneiden)
                    DECLARE @Date_plus10Minuten datetime;
                    SET @Date_plus10Minuten = DATEADD(MINUTE, 1, @Date);


                    SELECT 
	                    t_Kundendaten.datID
	                    , t_Kundendaten.wellenID
	                    , t_Kundendaten.projektID
	                    , t_Kundendaten.email
	                    , t_Kundendaten.finishedDate
	                    , DATEDIFF(SECOND, t_Kundendaten.finishedDate, @Date) AS [Reaktionszeit]
		
                    FROM t_Kundendaten
                    LEFT JOIN t_Batches ON t_Kundendaten.batchID = t_Batches.batchID
                    LEFT JOIN t_Sender ON t_Batches.senderID = t_Sender.senderID

                    WHERE t_Kundendaten.geloescht=0 AND t_Kundendaten.istVersandt = 1
	                    AND t_Kundendaten.email = @mailFrom
	                    AND t_Sender.mailadresse = @mailTo
	                    AND t_Kundendaten.finishedDate >= @Date_minus90Tage
	                    AND t_Kundendaten.finishedDate <= @Date_plus10Minuten
                    "
                    , new object[,] {
                        {"@mailFrom", mailFrom.ToLower()}
                        , {"@mailTo", mailTo.ToLower()}
                        , {"@Date", message.Date.LocalDateTime}
                    }
                );



                //Antwort in ersten 10 Sekunden --> vermutlich automatisch!
                if (!DataTableIsNullOrEmpty(dtKundendaten))
                {
                    versandDatum = tsLib.tsConverter.objectToDateTime(dtKundendaten.Rows[0]["finishedDate"]);
                    if (versandDatum.Value.Year == 1900) versandDatum = null;

                    ReaktionszeitSekunden = tsLib.tsConverter.objectToInt(dtKundendaten.Rows[0]["Reaktionszeit"], -999);


                    if (ReaktionszeitSekunden > -999 && ReaktionszeitSekunden < maxSekundenAutoreply && moveFolder != null)
                    {
                        //verschiebe ebenfalls in Auto-Reply-Ordner
                        folder.MoveTo(uid, moveFolder);
                        continue;
                    }
                }


                if (message.MessageId == null) continue;


                mailID = tsLib.tsConverter.objectToInt(dbcon.executeScalar(@"
                    SELECT mailID FROM t_Reply WHERE mailadrID = @mailadrID AND messageID = @messageID"
                            , new object[,] {
                            {"@mailadrID", mailadrID}
                            , {"@messageID", message.MessageId.ToString()}
                            }
                    ));



                if (mailID <= 0)
                {
                    inReplyTo = message.InReplyTo ?? "";
                    if (inReplyTo.Length > 0)
                    {
                        inReplyTo_MailID = tsLib.tsConverter.objectToInt(dbcon.executeScalar(@"
                            SELECT mailID FROM t_Reply WHERE mailadrID = @mailadrID AND messageID = @messageID"
                                , new object[,] {
                                    {"@mailadrID", mailadrID}
                                    , {"@messageID",inReplyTo}
                                 }
                            ));
                    }
                    else inReplyTo_MailID = -1;





                    mailID = tsLib.tsConverter.objectToInt(dbcon.executeScalar(@"
                                INSERT INTO t_Reply (
                                    mailadrID
                                    , folder
                                    , messageID, uid
                                    , mailFrom, mailTo, Cc, Bcc, Subject
                                    , Date
                                    , inReplyTo, inReplyTo_mailID
                                    , HtmlBody, TextBody
                                    , datID, wellenID, projektID, versandDatum, ReaktionSekunden
                                )
                                VALUES (
                                    @mailadrID
                                    , @folder
                                    , @messageID, @uid
                                    , @mailFrom, @mailTo, @Cc, @Bcc, @Subject
                                    , @Date
                                    , @inReplyTo, @inReplyTo_mailID
                                    , @HtmlBody, @TextBody
                                    , @datID, @wellenID, @projektID, @versandDatum, @ReaktionSekunden
                                );

                                SELECT @@IDENTITY;
                                "
                        , new object[,] {
                                    {"@mailadrID", mailadrID}
                                    , {"@folder",folder.Name}
                                    , {"@messageID", message.MessageId.ToString()}
                                    , {"@uid", tsLib.tsConverter.objectToInt(uid.ToString())}
                                    , {"@mailFrom", internetAddressListToString(message.From)}
                                    , {"@mailTo", internetAddressListToString(message.To)}
                                    , {"@Cc", internetAddressListToString(message.Cc)}
                                    , {"@Bcc", internetAddressListToString(message.Bcc)}
                                    , {"@Subject", message.Subject ?? ""}
                                    , {"@Date", message.Date.LocalDateTime}
                                    , {"@inReplyTo", inReplyTo ?? ""}
                                    , {"@inReplyTo_mailID", inReplyTo_MailID}
                                    , {"@HtmlBody", message.HtmlBody ?? ""}
                                    , {"@TextBody", message.TextBody ?? ""}

                                    , {"@datID", !DataTableIsNullOrEmpty(dtKundendaten) ? tsLib.tsConverter.objectToInt(dtKundendaten.Rows[0]["datID"], 0) : (object)System.DBNull.Value}
                                    , {"@wellenID", !DataTableIsNullOrEmpty(dtKundendaten) ? tsLib.tsConverter.objectToInt(dtKundendaten.Rows[0]["wellenID"], 0) : (object)System.DBNull.Value}
                                    , {"@projektID", !DataTableIsNullOrEmpty(dtKundendaten) ? tsLib.tsConverter.objectToInt(dtKundendaten.Rows[0]["projektID"], 0) : (object)System.DBNull.Value}
                                    , {"@versandDatum", versandDatum ??  (object)System.DBNull.Value}
                                    , {"@ReaktionSekunden", ReaktionszeitSekunden > -999 ? ReaktionszeitSekunden : (object)System.DBNull.Value}
                                }));

                    if (mailID > 0) update = true;

                    //Anhänge herunterladen und in DB
                    foreach (var attachment in message.Attachments)
                    {
                        saveAttachment(dbcon, attachment, mailadrID, email, mailID, false);
                    }

                    //Inline-Bilder herunterladen und in DB
                    foreach (var image in message.BodyParts.OfType<MimeKit.MimePart>().Where(x => x.ContentType.IsMimeType("image", "*")).ToList())
                    {
                        saveAttachment(dbcon, image, mailadrID, email, mailID, true);
                    }
                } //ENDE neue Nachricht


                //Nachricht wurde bereits gespeichert
                else continue;
            }

            folder.Close();

            return update;

        } //ENDE getMailsForFolder






        /*
         * aktualisiere ein bestimmtes Mailkonto, übergeben wird nur die Mailadresse
         */

        public static void getAllMails(tsLib.dbConnector dbcon, string mailadresse)
        {
            int mailadrID = tsLib.tsConverter.objectToInt(dbcon.executeScalar("SELECT mailadrID FROM t_ReplyAdressen WHERE email=@email"
                , new object[,] {
                    {"@email", mailadresse}
                }
            ));
            if (mailadrID <= 0) return;

            getAllMails(dbcon, mailadrID);

        } //ENDE getAllMAils - nur über die Mailadresse




        /*
         * aktualisiere ein bestimmtes Mailkonto, übergeben wird nur die ID
         */

        public static void getAllMails(tsLib.dbConnector dbcon, int mailadrID)
        {
            string serverName = tsLib.tsConverter.objectToString(dbcon.executeScalar("SELECT serverName FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));
            int port = tsLib.tsConverter.objectToInt(dbcon.executeScalar("SELECT port FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));
            string email = tsLib.tsConverter.objectToString(dbcon.executeScalar("SELECT email FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));

            //Passwort auslesen
            string password = tsLib.tsConverter.objectToString(dbcon.executeScalar(@"
                SELECT 
                    CAST(DecryptByPassphrase(@passphrase, [password]) AS nvarchar(max)) 
                FROM t_ReplyAdressen
                WHERE mailadrID = @mailadrID
                "
                , new object[,] {
                    {"@passphrase", imap.pwdMailEncryptPassphrase}
                    , {"@mailadrID", mailadrID}
                }));

            getAllMails(dbcon, mailadrID, serverName, port, email, password);

        } //ENDE getAllMAils - nur über die ID




        /*
         * aktualisiere ein bestimmtes Mailkonto, übergeben wird die ID und einige Basis-Settings
         */

        public static void getAllMails(tsLib.dbConnector dbcon, int mailadrID, string serverName, int port, string email, string password)
        {

            ImapClient client = new ImapClient();

            //Verbindung zum Mailkonto aufbauen
            try
            {
                client.Connect(serverName, port, true /*SSL*/);


                // Note: since we don't have an OAuth2 token, disable
                // the XOAUTH2 authentication mechanism.
                client.AuthenticationMechanisms.Remove("XOAUTH2");

                //Login
                client.Authenticate(email, password);
            }
            catch
            {
                Log(dbcon, mailadrID, "Verbindung zum Konto " + email + " fehlgeschlagen");
                return;
            }



            //wann wurden zuletzt Mails abgerufen?
            DateTime letztesUpdate = tsLib.tsConverter.objectToDateTime(dbcon.executeScalar(@"
                        SELECT letztesUpdate FROM t_ReplyAdressen WHERE mailadrID = " + mailadrID)).AddMinutes(-10); //kleine Überlappung zur Sicherheit

            MailKit.Search.SearchQuery queryNewMails = letztesUpdate.Year > 1900 ? MailKit.Search.SearchQuery.DeliveredAfter(letztesUpdate) : MailKit.Search.SearchQuery.All;


            bool update = false;
            bool autoReplyExists = false;
            foreach (IMailFolder folder in client.GetFolders(client.PersonalNamespaces[0]))
            {
                // Debug.WriteLine("FOLDER? "  + folder.Name);

                //neue Mails laden
                update = update | folder_getMails(dbcon, mailadrID, email, folder, queryNewMails.And(MailKit.Search.SearchQuery.NotDeleted));

                //gelesen bzw. beantwortet - Flags laden
                folder_updateFlags(dbcon, mailadrID, folder);

                if (folder.Name == folderNameAutoreply) autoReplyExists = true;
            }

            //Auto-Reply-Folder prüfen falls gerade erst angelegt
            if (!autoReplyExists)
            {
                IMailFolder autoReplyFolder = client.GetFolder(client.PersonalNamespaces[0]).GetSubfolder(folderNameAutoreply);

                //neue Mails laden
                update = update | folder_getMails(dbcon, mailadrID, email, autoReplyFolder, queryNewMails.And(MailKit.Search.SearchQuery.NotDeleted));

                //gelesen bzw. beantwortet - Flags laden
                folder_updateFlags(dbcon, mailadrID, autoReplyFolder);
            }


            //letzte Änderungen zurück in DB
            if (update)
            {
                dbcon.executeNonQuery(@"
                            UPDATE t_ReplyAdressen 
                            SET 
                                letztesUpdate = GETDATE()
                            WHERE mailadrID = " + mailadrID);
            }

            client.Disconnect(true);

        } //ENDE getAllMails - für bestimmte mailadrID + Parameter



        public static void forwardMessage(tsLib.dbConnector dbcon, int mailadrID, int mailID, string mailadresse)
        {
            string serverName = tsLib.tsConverter.objectToString(dbcon.executeScalar("SELECT serverName FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));
            int port = tsLib.tsConverter.objectToInt(dbcon.executeScalar("SELECT port FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));
            string email = tsLib.tsConverter.objectToString(dbcon.executeScalar("SELECT email FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));

            //Passwort auslesen
            string password = tsLib.tsConverter.objectToString(dbcon.executeScalar(@"
                SELECT 
                    CAST(DecryptByPassphrase(@passphrase, [password]) AS nvarchar(max)) 
                FROM t_ReplyAdressen
                WHERE mailadrID = @mailadrID
                "
                , new object[,] {
                    {"@passphrase", imap.pwdMailEncryptPassphrase}
                    , {"@mailadrID", mailadrID}
                }));

            ImapClient client = new ImapClient();

            //Verbindung zum Mailkonto aufbauen
            try
            {
                client.Connect(serverName, port, true /*SSL*/);


                // Note: since we don't have an OAuth2 token, disable
                // the XOAUTH2 authentication mechanism.
                client.AuthenticationMechanisms.Remove("XOAUTH2");

                //Login
                client.Authenticate(email, password);
            }
            catch
            {
                Log(dbcon, mailadrID, "Verbindung zum Konto " + email + " fehlgeschlagen");
                return;
            }

            //ENDE Verbindung herstellen



            //welcher Folder?
            IMailFolder mailFolder = null;
            string strFolder = tsLib.tsConverter.objectToString(dbcon.executeScalar("SELECT folder FROM t_Reply WHERE mailID=" + mailID));

            //Suche den benötigten Ordner
            foreach (IMailFolder folder in client.GetFolders(client.PersonalNamespaces[0]))
            {
                if (folder.Name.ToLower() == strFolder.ToLower()) mailFolder = folder;
            }


            mailFolder.Open(FolderAccess.ReadWrite);

            //welche UID?
            var range = new UniqueIdRange(new UniqueId((uint)tsLib.tsConverter.objectToInt(dbcon.executeScalar("SELECT uid FROM t_Reply WHERE mailID=" + mailID))), UniqueId.MaxValue);
            MimeMessage message = null;

            foreach (var uid in mailFolder.Search(range, MailKit.Search.SearchQuery.All))
            {
                message = mailFolder.GetMessage(uid);
                mailFolder.SetFlags(uid, MessageFlags.Seen, true);
                break;
            }

            //Flags + DB aktualisieren
            folder_updateFlags(dbcon, mailadrID, mailFolder);

            mailFolder.Close();


            if (message == null)
            {
                client.Disconnect(true);
                return;
            }


            using (var smtp_client = new MailKit.Net.Smtp.SmtpClient())
            {
                smtp_client.Connect(serverName, 465, true);
                smtp_client.Authenticate(email, password);

                var sender = new MailboxAddress(email);
                var recipients = new[] { new MailboxAddress(mailadresse) };

                smtp_client.Send(message, sender, recipients);

                smtp_client.Disconnect(true);
            }




            client.Disconnect(true);

        } //ENDE forwardMessage




        /*
            Nachricht in anderen Folder verschieben
        */

        public static void MoveToFolder(tsLib.dbConnector dbcon, int mailadrID, int mailID, string folderName)
        {
            string serverName = tsLib.tsConverter.objectToString(dbcon.executeScalar("SELECT serverName FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));
            int port = tsLib.tsConverter.objectToInt(dbcon.executeScalar("SELECT port FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));
            string email = tsLib.tsConverter.objectToString(dbcon.executeScalar("SELECT email FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));

            //Passwort auslesen
            string password = tsLib.tsConverter.objectToString(dbcon.executeScalar(@"
                SELECT 
                    CAST(DecryptByPassphrase(@passphrase, [password]) AS nvarchar(max)) 
                FROM t_ReplyAdressen
                WHERE mailadrID = @mailadrID
                "
                , new object[,] {
                    {"@passphrase", imap.pwdMailEncryptPassphrase}
                    , {"@mailadrID", mailadrID}
                }));

            ImapClient client = new ImapClient();

            //Verbindung zum Mailkonto aufbauen
            try
            {
                client.Connect(serverName, port, true /*SSL*/);

                // Note: since we don't have an OAuth2 token, disable
                // the XOAUTH2 authentication mechanism.
                client.AuthenticationMechanisms.Remove("XOAUTH2");

                //Login
                client.Authenticate(email, password);
            }
            catch
            {
                Log(dbcon, mailadrID, "Verbindung zum Konto " + email + " fehlgeschlagen");
                return;
            }

            //ENDE Verbindung herstellen


            IMailFolder startFolder = null, moveFolder = null;

            //aus welchem Folder wird wegverschoben
            string strStartFolder = tsLib.tsConverter.objectToString(dbcon.executeScalar("SELECT folder FROM t_Reply WHERE mailID=" + mailID));
            if (strStartFolder == folderName) return;

            //Suche den benötigten Ordner
            foreach (IMailFolder folder in client.GetFolders(client.PersonalNamespaces[0]))
            {
                if (folder.Name.ToLower() == folderName.ToLower()) moveFolder = folder;
                if (folder.Name.ToLower() == strStartFolder.ToLower()) startFolder = folder;
            }

            //Falls nicht vorhanden --> anlegen
            if (moveFolder == null)
            {
                try
                {
                    moveFolder = client.GetFolders(client.PersonalNamespaces[0])[0].ParentFolder.Create(folderName, true);
                }
                catch
                {
                    Log(dbcon, mailadrID, "Fehler beim Anlegen des Folders '" + folderName + "' im Mailkonto");
                    return;
                }
            }

            if (startFolder == null || moveFolder == null) return;


            //welcher Folder ist das? anlegen falls nicht vorhanden in DB
            int folderID = tsLib.tsConverter.objectToInt(dbcon.executeScalar(@"
                IF (SELECT COUNT(*) FROM t_ReplyFolders WHERE mailadrID = @mailadrID AND folderName = @folderName) = 0
                BEGIN
                    INSERT INTO t_ReplyFolders(mailadrID, folderName)
                    VALUES (@mailadrID, @folderName);

                    SELECT @@IDENTITY;
                END
                ELSE 
                BEGIN
                    SELECT TOP 1 folderID 
                    FROM t_ReplyFolders 
                    WHERE mailadrID = @mailadrID AND folderName = @folderName
                END
                "
                , new object[,] {
                                {"@mailadrID", mailadrID}
                                , {"@folderName", moveFolder.Name}
                             }
                ));

            if (folderID <= 0)
            {
                Log(dbcon, mailadrID, "Fehler beim Anlegen des Folders '" + moveFolder.Name + "' in der Datenbank");
                return;
            }


            /*
             * eigentliches Verschieben
             */

            startFolder.Open(FolderAccess.ReadWrite);

            //welche UID?
            var range = new UniqueIdRange(new UniqueId((uint)tsLib.tsConverter.objectToInt(dbcon.executeScalar("SELECT uid FROM t_Reply WHERE mailID=" + mailID))), UniqueId.MaxValue);

            foreach (var uid in startFolder.Search(range, MailKit.Search.SearchQuery.All))
            {
                startFolder.AddFlags(uid, MessageFlags.Seen, true);

                //var message = folder.GetMessage(uid);
                startFolder.MoveTo(uid, moveFolder);
                break;
            }

            startFolder.Close();


            //Flags + DB aktualisieren
            folder_updateFlags(dbcon, mailadrID, moveFolder);

            client.Disconnect(true);

        } //ENDE ChangeFolder




        /*
         * Nachricht als gelesen markieren
         */

        public static void markAsSeen(tsLib.dbConnector dbcon, int mailadrID, int mailID)
        {
            string serverName = tsLib.tsConverter.objectToString(dbcon.executeScalar("SELECT serverName FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));
            int port = tsLib.tsConverter.objectToInt(dbcon.executeScalar("SELECT port FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));
            string email = tsLib.tsConverter.objectToString(dbcon.executeScalar("SELECT email FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));

            //Passwort auslesen
            string password = tsLib.tsConverter.objectToString(dbcon.executeScalar(@"
                SELECT 
                    CAST(DecryptByPassphrase(@passphrase, [password]) AS nvarchar(max)) 
                FROM t_ReplyAdressen
                WHERE mailadrID = @mailadrID
                "
                , new object[,] {
                    {"@passphrase", imap.pwdMailEncryptPassphrase}
                    , {"@mailadrID", mailadrID}
                }));

            ImapClient client = new ImapClient();

            //Verbindung zum Mailkonto aufbauen
            try
            {
                client.Connect(serverName, port, true /*SSL*/);


                // Note: since we don't have an OAuth2 token, disable
                // the XOAUTH2 authentication mechanism.
                client.AuthenticationMechanisms.Remove("XOAUTH2");

                //Login
                client.Authenticate(email, password);
            }
            catch
            {
                Log(dbcon, mailadrID, "Verbindung zum Konto " + email + " fehlgeschlagen");
                return;
            }

            //ENDE Verbindung herstellen



            //welcher Folder?
            IMailFolder mailFolder = null;
            string strFolder = tsLib.tsConverter.objectToString(dbcon.executeScalar("SELECT folder FROM t_Reply WHERE mailID=" + mailID));

            //Suche den benötigten Ordner
            foreach (IMailFolder folder in client.GetFolders(client.PersonalNamespaces[0]))
            {
                if (folder.Name.ToLower() == strFolder.ToLower()) mailFolder = folder;
            }


            mailFolder.Open(FolderAccess.ReadWrite);



            //welche UID?
            var range = new UniqueIdRange(new UniqueId((uint)tsLib.tsConverter.objectToInt(dbcon.executeScalar("SELECT uid FROM t_Reply WHERE mailID=" + mailID))), UniqueId.MaxValue);

            foreach (var uid in mailFolder.Search(range, MailKit.Search.SearchQuery.All))
            {
                mailFolder.SetFlags(uid, MessageFlags.Seen, true);
                break;
            }

            //Flags + DB aktualisieren
            folder_updateFlags(dbcon, mailadrID, mailFolder);

            mailFolder.Close();

            client.Disconnect(true);

        } //ENDE markAsSeen





        /*
         * ohne Parameter --> prüft alle Mailkonten
         */

        public static void getAllMails(tsLib.dbConnector dbcon)
        {
            DataTable dt = dbcon.SqlToDataTable(@"
                SELECT 
                    t_ReplyAdressen.*
                    , CAST(DecryptByPassphrase(@passphrase, [password]) AS nvarchar(max)) AS [password_enc]
                FROM t_ReplyAdressen 
                WHERE geloescht = 0 AND inaktiv = 0"

                , new object[,] {
                    {"@passphrase", imap.pwdMailEncryptPassphrase}
                }
            );


            if (dt != null)
            {
                int mailadrID = -1;

                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    Debug.Write("CHECK " + dt.Rows[i]["email"].ToString() + "...");

                    mailadrID = tsLib.tsConverter.objectToInt(dt.Rows[i]["mailadrID"]);
                    if (mailadrID <= 0) continue;

                    getAllMails(
                        dbcon
                        , mailadrID
                        , dt.Rows[i]["serverName"].ToString()
                        , tsLib.tsConverter.objectToInt(dt.Rows[i]["port"], 993)
                        , dt.Rows[i]["email"].ToString()
                        , dt.Rows[i]["password_enc"].ToString()
                    );
                }
            }

        } //ENDE checkMails



        //-------------------------------------------------------------------------------

        public static void MailAliasAnlegen(mySqlConnector mycon, string email, string weiterleitungen)
        {
            int anzAlias = tsLib.tsConverter.objectToInt(mycon.executeScalar(@"
                    SELECT COUNT(*) 
                    FROM alias 
                    WHERE address = @address
                "
                , new object[,] {
                    {"@address", email}
                }));

            //Debug.WriteLine("anzAlias? " + anzAlias);

            //neuen Alias anlegen
            if (anzAlias <= 0)
            {
                mycon.executeNonQuery(@"
                    INSERT INTO alias (address, goto, domain, created) 
                    VALUES (@address, @goto, @domain, NOW())
                    "

                    , new object[,] {
                        {"@goto", weiterleitungen}
                        , {"@address", email}
                        , {"@domain", email.Substring(email.IndexOf('@') + 1)}
                    });
            }

            //vorhandenen Alias aktualisieren
            else
            {
                mycon.executeNonQuery(@"
                    UPDATE alias
                    SET 
                        goto = @goto
                        , modified = NOW()
                    WHERE address = @address
                "
                    , new object[,] {
                    {"@goto", weiterleitungen}
                    , {"@address", email}
                });
            }
        } //ENDE MailAliasAnlegen



        //--------------------------------------------------------------------------------------------------------------------



        /*
         * lösche alle Nachrichten von einem bestimmten Mailkonto
         */

        public static void deleteAllMails(tsLib.dbConnector dbcon, int mailadrID, DateTime? olderThan = null)
        {
            string serverName = tsLib.tsConverter.objectToString(dbcon.executeScalar("SELECT serverName FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));
            int port = tsLib.tsConverter.objectToInt(dbcon.executeScalar("SELECT port FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));
            string email = tsLib.tsConverter.objectToString(dbcon.executeScalar("SELECT email FROM t_ReplyAdressen WHERE mailadrID=" + mailadrID));

            //Passwort auslesen
            string password = tsLib.tsConverter.objectToString(dbcon.executeScalar(@"
                SELECT 
                    CAST(DecryptByPassphrase(@passphrase, [password]) AS nvarchar(max)) 
                FROM t_ReplyAdressen
                WHERE mailadrID = @mailadrID
                "
                , new object[,] {
                    {"@passphrase", imap.pwdMailEncryptPassphrase}
                    , {"@mailadrID", mailadrID}
                }));

            deleteAllMails(dbcon, mailadrID, serverName, port, email, password, olderThan);

        } //ENDE getAllMAils - nur über die ID





        /*
         * aktualisiere ein bestimmtes Mailkonto, übergeben wird die ID und einige Basis-Settings
         */

        public static void deleteAllMails(tsLib.dbConnector dbcon, int mailadrID, string serverName, int port, string email, string password, DateTime? olderThan = null)
        {

            ImapClient client = new ImapClient();

            //Verbindung zum Mailkonto aufbauen
            try
            {
                client.Connect(serverName, port, true /*SSL*/);
            }
            catch
            {
                Log(dbcon, mailadrID, "Verbindung zum Konto " + email + " fehlgeschlagen");
                return;
            }


            // Note: since we don't have an OAuth2 token, disable
            // the XOAUTH2 authentication mechanism.
            client.AuthenticationMechanisms.Remove("XOAUTH2");

            //Login
            client.Authenticate(email, password);


            foreach (IMailFolder folder in client.GetFolders(client.PersonalNamespaces[0]))
            {
                //Zugriff auf den Folder
                folder.Open(FolderAccess.ReadWrite);


                foreach (var uid in folder.Search(olderThan != null ? MailKit.Search.SearchQuery.NotDeleted.And(MailKit.Search.SearchQuery.DeliveredBefore((DateTime)olderThan)) : MailKit.Search.SearchQuery.NotDeleted))
                {
                    var message = folder.GetMessage(uid);
                    folder.AddFlags(uid, MessageFlags.Deleted, true);

                    if (message == null) continue;
                    if (message.MessageId == null) continue;

                    //Debug.WriteLine("DELETE " + message.Date + " | " + message.From + " | " + message.MessageId + " | " + folder.Name);

                    dbcon.executeNonQuery(@"
                        --erst die Anhänge löschen (Join nachher ja nicht mehr möglich)
                        DELETE t_ReplyAttachments
                        FROM t_ReplyAttachments
                        LEFT JOIN t_Reply ON t_ReplyAttachments.mailID = t_Reply.mailID
                        WHERE t_Reply.folder = @folder AND t_Reply.messageID = @messageID

                        --dann die echten Daten löschen
                        DELETE FROM t_Reply
                        WHERE folder = @folder AND messageID = @messageID

                        ", new object[,] {
                            {"@folder", folder.Name}
                            , {"@messageID", message.MessageId}
                         });
                }

            }

            client.Disconnect(true);

        } //ENDE getAllMails - für bestimmte mailadrID + Parameter
    }
}
