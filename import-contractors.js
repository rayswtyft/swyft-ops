const fs = require("fs");
const path = require("path");

const DB_FILE = path.join(__dirname, "db.json");

const contractorsToImport = [
  { companyName: "10 Marion Avenue", contactName: "10 Marion Avenue", billingAddress: "10 Marion Avenue, Howell Township, NJ 07731", phone: "(845) 633-0664", email: "18skah@gmail.com" },
  { companyName: "1040 Central Avenue", contactName: "1040 Central Avenue", billingAddress: "1040 Central Avenue, Lakewood, NJ 08701", phone: "9292130975", email: "Manniegibber@gmail.com" },
  { companyName: "", contactName: "1613 Beacon", billingAddress: "1613 Beacon Street, Toms River, NJ 08757", phone: "", email: "Phillip@pmsco.cpa" },
  { companyName: "", contactName: "Ahuva Golombeck", billingAddress: "8 Weston Court, Jackson, NJ 08527", phone: "(732) 882-8081", email: "Lubowskya@gmail.com" },
  { companyName: "Attune Insurance", contactName: "Attune Insurance", billingAddress: "", phone: "", email: "" },
  { companyName: "Sterling Forest Shul Ashkenaz", contactName: "Chaim Kogel", billingAddress: "144 Hadassah Lane, Lakewood, NJ 08701", phone: "(347) 731-1820", email: "info@sterlingforest.org" },
  { companyName: "", contactName: "Chavi Shapiro", billingAddress: "26 Citadel Drive, Jackson, NJ", phone: "(347) 906-2584", email: "Chavishap1@gmail.com" },
  { companyName: "CMR Design", contactName: "CMR Design", billingAddress: "", phone: "(848) 400-8161", email: "Orders@cmr-design.com" },
  { companyName: "GSL Management", contactName: "GSL Management", billingAddress: "24 Shayas Rd, Lakewood, NJ 08701", phone: "7322784545", email: "leorazoolay@gmail.com" },
  { companyName: "", contactName: "Hershy Gluck", billingAddress: "205 Hadassah Lane, Lakewood, NJ 08701", phone: "(347) 675-5629", email: "Leahgluck41@gmail.com" },
  { companyName: "Howell Township Development LLC", contactName: "Howell Township Development LLC", billingAddress: "1515 Pine Street Suite 202, Lakewood, NJ 08701", phone: "732-276-9525 x503", email: "Tova@thetroisgroup.com" },
  { companyName: "", contactName: "Ilana Maslaton", billingAddress: "6 Gvuras Ari Drive, Lakewood, NJ 08701", phone: "(917) 771-8002", email: "iym201@nyu.edu" },
  { companyName: "Integris Contracting", contactName: "Integris Contracting", billingAddress: "", phone: "7328598483", email: "Moshe@integrissvc.com" },
  { companyName: "Lakewood Painting and Remodeling", contactName: "", billingAddress: "", phone: "(732) 995-3481", email: "office@redkeymanagement.com" },
  { companyName: "", contactName: "Malkiel Busu", billingAddress: "287 Cathy Lane, Lakewood, NJ 08701", phone: "(917) 628-7515", email: "Malkielbussu26@gmail.com" },
  { companyName: "", contactName: "Mordechai Berger", billingAddress: "17 Georgian Blvd, Jackson, NJ 08527", phone: "", email: "bernsteinbracha126@gmail.com" },
  { companyName: "", contactName: "Mordechai Miller", billingAddress: "", phone: "(718) 288-4317", email: "" },
  { companyName: "", contactName: "Mr Brecher", billingAddress: "", phone: "(848) 362-1402", email: "brecherel@gmail.com" },
  { companyName: "", contactName: "Mr. Fogel", billingAddress: "17 Elana Drive, Jackson, NJ 08527", phone: "(732) 966-2594", email: "72fogel@gmail.com" },
  { companyName: "", contactName: "Mrs Zafrani", billingAddress: "41 Cambridge Drive, Jackson, NJ 08527", phone: "(917) 538-6544", email: "Rachelihr99@gmail.com" },
  { companyName: "", contactName: "Shimon Kamin", billingAddress: "", phone: "(732) 874-1527", email: "shmnka111@gmail.com" },
  { companyName: "", contactName: "Shlomo Berney", billingAddress: "", phone: "(917) 533-0907", email: "Shloberney@gmail.com" },
  { companyName: "The Flooring People", contactName: "The Flooring People", billingAddress: "", phone: "(718) 541-2961", email: "josh@theflooringpeoplenj.com" },
  { companyName: "Way To Go New Jersey", contactName: "Way To Go New Jersey", billingAddress: "", phone: "(973) 913-4875", email: "Billing@waytogonj.com" },
  { companyName: "", contactName: "Yenny Mermel", billingAddress: "9 Lenox Drive, Lakewood, NJ 08701", phone: "(732) 569-4565", email: "yennymermel@gmail.com" },
  { companyName: "", contactName: "Yoni Braun", billingAddress: "", phone: "", email: "eliezerjbraun@gmail.com" }
];

const db = JSON.parse(fs.readFileSync(DB_FILE, "utf8"));
db.contractors = db.contractors || [];

let nextId = db.contractors.length
  ? Math.max(...db.contractors.map(c => Number(c.id) || 0)) + 1
  : 1;

for (const contractor of contractorsToImport) {
  db.contractors.push({
    id: nextId++,
    companyName: contractor.companyName,
    contactName: contractor.contactName,
    billingAddress: contractor.billingAddress,
    phone: contractor.phone,
    email: contractor.email,
    paymentTerms: "",
    createdAt: new Date().toISOString()
  });
}

fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));

console.log("✅ Contractors imported successfully");