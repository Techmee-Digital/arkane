
document.addEventListener("DOMContentLoaded",function(){
  let chk = document.getElementById("show-only-dups");
  if(chk){
    chk.addEventListener("change",function(){
      let rows = document.querySelectorAll("tbody tr[data-duplicate]");
      rows.forEach(r=>{
        let isDup = r.getAttribute("data-duplicate")==="true";
        r.style.display = (!chk.checked || isDup) ? "" : "none";
      });
    });
  }
  
  let showPw = document.getElementById("show-password");
  if(showPw){
    showPw.addEventListener("change",function(){
      let pw = document.getElementById("pw");
      pw.type = this.checked ? "text" : "password";
    });
  }
});
